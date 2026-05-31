from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.models.param import Param
from datetime import datetime, timedelta
import sys
import os
import time

# Thêm đường dẫn src và config vào sys.path
airflow_home = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
sys.path.append(airflow_home) # Để import config (nếu config nằm ở /opt/airflow/config)
sys.path.append(os.path.join(airflow_home, 'dags')) # Để import src (nếu src nằm ở /opt/airflow/dags/src)

from src.utils.gee_quota import check_gee_quota, get_pending_tasks
from src.utils.gee_coordinator import wait_for_tasks
from src.utils.gcs_scan import check_bucket
from src.utils.gee_utils import get_date_range, get_satellite_dates, generate_date_chunks
from src.process.gee_export import export_to_bucket
from src.process.file_download import run_download_pipeline
from src.auth import initialize_gee
from config.config import ROIS, SATELLITE_CONFIG, GCS_CONFIG
import ee

# ==============================================================================
# 1. CẤU HÌNH DAG & PARAMS
# ==============================================================================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'download_pipeline',
    default_args=default_args,
    description='Pipeline tối ưu: Sensor -> Export (Multi-Mode) -> Wait -> Download',
    schedule_interval='0 2 5 * *', # Mặc định chạy 2h sáng ngày 5 hàng tháng (cho mode monthly)
    catchup=False,
    tags=['download', 'unified', 'optimized', 'v2'],
    params={
        'mode': Param('monthly', enum=['yearly', 'historical', 'monthly'], description="Chế độ chạy"),
        'start_date': Param('2023-01-01', type='string', format='date', description="Ngày bắt đầu (cho mode monthly/yearly)"),
        'end_date': Param('2023-01-31', type='string', format='date', description="Ngày kết thúc (cho mode monthly/yearly)"),
    }
)

# ==============================================================================
# 2. CÁC HÀM TASK (CALLABLE)
# ==============================================================================

def sensor_check_quota(**kwargs):
    """
    Sensor: Kiểm tra Quota GEE.
    """
    count = check_gee_quota()
    if count > 2800:
        print(f"⚠️ [SENSOR] Quá tải ({count} tasks). Chờ 10 phút...")
        return False
    print(f"✅ [SENSOR] Hệ thống rảnh ({count} tasks). Tiếp tục.")
    return True

def task_export(**kwargs):
    """
    Task Export: Xử lý logic theo Mode -> Quét GCS -> Gửi lệnh Export.
    """
    # 1. Lấy Params & Mode
    # 1. Lấy Params & Mode
    params = kwargs['params']
    dag_run = kwargs.get('dag_run')
    
    # Xác định kiểu chạy (Scheduled hay Manual)
    # Nếu là scheduled -> run_type='scheduled' -> get_date_range sẽ lấy tháng trước
    run_type_str = 'scheduled' if dag_run and dag_run.run_type == 'scheduled' else 'manual'
    
    mode = params.get('mode', 'monthly')
    print(f"🚀 [START] Bắt đầu Export với chế độ: {mode.upper()} (Run Type: {run_type_str})")
    
    initialize_gee()
    
    bucket_name = GCS_CONFIG['bucket_name']
    base_folder = GCS_CONFIG['base_folder']
    
    # 2. Scan Once (Tối ưu)
    existing_files = check_bucket(bucket_name, base_folder)
    
    # [NEW] Get Pending Tasks (Tránh duplicate)
    pending_tasks = get_pending_tasks()
    
    submitted_tasks = []

    # --------------------------------------------------------------------------
    # BƯỚC 2.5: TÍNH TOÁN TỔNG SỐ TASK (DRY RUN)
    # --------------------------------------------------------------------------
    total_estimated_tasks = 0
    print("🔄 [ESTIMATE] Đang tính toán tổng số task dự kiến...")
    
    # Xác định Interval dựa trên Mode
    interval = 'yearly' if mode == 'yearly' else 'monthly'
    print(f"ℹ️ [INFO] Chế độ gộp (Composite Interval): {interval.upper()}")

    for city_name, roi_path in ROIS.items():
        for sat_key, sat_config in SATELLITE_CONFIG.items():
            # Logic lấy ngày y hệt như bên dưới
            s_date, e_date = get_date_range(mode, params, run_type=run_type_str)
            if mode == 'historical':
                s_date, e_date = get_satellite_dates(sat_config['id'])
            
            # Sử dụng generator chung
            for _, _ in generate_date_chunks(s_date, e_date, interval=interval):
                total_estimated_tasks += 1

    print(f"📊 [ESTIMATE] TỔNG SỐ TASK DỰ KIẾN: {total_estimated_tasks}")
    # --------------------------------------------------------------------------
    
    # 3. Duyệt qua từng ROI và Vệ tinh
    for city_name, roi_path in ROIS.items():
        roi = ee.FeatureCollection(roi_path)
        
        for sat_key, sat_config in SATELLITE_CONFIG.items():
            
            # 4. Xác định khoảng thời gian (Date Logic)
            s_date, e_date = get_date_range(mode, params, run_type=run_type_str)
            
            # Xử lý đặc biệt cho mode HISTORICAL
            if mode == 'historical':
                s_date, e_date = get_satellite_dates(sat_config['id'])
                print(f"  ℹ️ [HISTORICAL] {sat_key}: {s_date} -> {e_date}")
            
            # 5. Loop qua từng chunk thời gian (sử dụng generator chung)
            for chunk_start_str, chunk_end_str in generate_date_chunks(s_date, e_date, interval=interval):
                
                # --- BATCHING LOGIC (Optimized for High Throughput) ---
                while True:
                    current_tasks = check_gee_quota()
                    # Giữ hàng đợi ở mức ~2800 để tối đa hóa tốc độ mà vẫn an toàn
                    if current_tasks < 2800:
                        break # Safe to submit
                    print(f"⏳ [QUOTA FULL] {current_tasks}/3000 tasks running. Waiting 5 minute...")
                    time.sleep(300) # Wait 5 minute (Fast retry)
                # -------------------------------------

                # Gọi hàm export
                task_id = export_to_bucket(
                    city_name=city_name,
                    roi=roi,
                    collection_info=sat_config,
                    start_date=chunk_start_str,
                    end_date=chunk_end_str,
                    bucket_name=bucket_name,
                    base_folder_in_bucket=base_folder,
                    existing_files=existing_files,
                    pending_tasks=pending_tasks # <--- Truyền danh sách pending
                )
                
                if task_id and task_id != "SKIPPED":
                    submitted_tasks.append(task_id)
                
    # 5. Trả về danh sách task ID
    print(f"📊 [SUMMARY] Đã gửi {len(submitted_tasks)} tasks lên GEE.")
    return submitted_tasks

def task_wait_completion(**kwargs):
    """
    Task Wait: Chờ task hoàn thành.
    """
    ti = kwargs['ti']
    submitted_tasks = ti.xcom_pull(task_ids='export')
    wait_for_tasks(submitted_tasks)

    # [NEW] Chờ cho đến khi hệ thống GEE hoàn toàn rảnh (0 task running)
    # Đảm bảo không còn task nào (kể cả của process khác) đang chạy trước khi download
    print("⏳ [WAIT] Đang chờ hệ thống GEE xử lý hết toàn bộ task (Global Wait)...")
    while True:
        count = check_gee_quota()
        if count == 0:
            print("✅ [DONE] Hệ thống GEE đã sạch task (0 running). Chuyển sang Download.")
            break
        print(f"    ... Vẫn còn {count} task đang chạy trên GEE. Chờ 60s...")
        time.sleep(60)

def task_download_local(**kwargs):
    """
    Task Download: Tải file về máy.
    """
    run_download_pipeline()

# ==============================================================================
# 3. ĐỊNH NGHĨA OPERATORS & LUỒNG
# ==============================================================================

with dag:
    # sensor = PythonSensor(
    #     task_id='check_gee_quota',
    #     python_callable=sensor_check_quota,
    #     mode='reschedule',
    #     poke_interval=600,
    #     timeout=3600 * 24
    # )
    
    # export = PythonOperator(
    #     task_id='gee_export',
    #     python_callable=task_export,
    # )
    
    # wait = PythonOperator(
    #     task_id='wait_for_completion',
    #     python_callable=task_wait_completion,
    # )
    
    download = PythonOperator(
        task_id='download_to_local',
        python_callable=task_download_local,
    )
    
download

