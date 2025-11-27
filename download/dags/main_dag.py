from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.models.param import Param
from datetime import datetime, timedelta
import sys
import os

# Thêm đường dẫn src và config vào sys.path
airflow_home = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
sys.path.append(airflow_home) # Để import config (nếu config nằm ở /opt/airflow/config)
sys.path.append(os.path.join(airflow_home, 'dags')) # Để import src (nếu src nằm ở /opt/airflow/dags/src)

from src.utils.gee_quota import check_gee_quota
from src.utils.gee_coordinator import wait_for_tasks
from src.utils.gcs_scan import check_bucket
from src.utils.gee_utils import get_date_range, get_satellite_dates
from src.process.gee_export import export_single_period
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
    'gee_pipeline',
    default_args=default_args,
    description='Pipeline tối ưu: Sensor -> Export (Multi-Mode) -> Wait -> Download',
    schedule_interval='0 2 5 * *', # Mặc định chạy 2h sáng ngày 5 hàng tháng (cho mode monthly)
    catchup=False,
    tags=['gee', 'unified', 'optimized', 'v2'],
    params={
        'mode': Param('monthly', enum=['monthly', 'yearly', 'historical', 'custom'], description="Chế độ chạy"),
        'start_date': Param('2023-01-01', type='string', format='date', description="Ngày bắt đầu (cho mode custom)"),
        'end_date': Param('2023-01-31', type='string', format='date', description="Ngày kết thúc (cho mode custom)"),
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
    if count > 2500:
        print(f"⚠️ [SENSOR] Quá tải ({count} tasks). Chờ 10 phút...")
        return False
    print(f"✅ [SENSOR] Hệ thống rảnh ({count} tasks). Tiếp tục.")
    return True

def task_export_unified(**kwargs):
    """
    Task Export: Xử lý logic theo Mode -> Quét GCS -> Gửi lệnh Export.
    """
    # 1. Lấy Params & Mode
    params = kwargs['params']
    mode = params.get('mode', 'monthly')
    print(f"🚀 [START] Bắt đầu Export với chế độ: {mode.upper()}")
    
    initialize_gee()
    
    bucket_name = GCS_CONFIG['bucket_name']
    base_folder = GCS_CONFIG['base_folder']
    
    # 2. Scan Once (Tối ưu)
    existing_files = check_bucket(bucket_name, base_folder)
    
    submitted_tasks = []
    
    # 3. Duyệt qua từng ROI và Vệ tinh
    for city_name, roi_path in ROIS.items():
        roi = ee.FeatureCollection(roi_path)
        
        for sat_key, sat_config in SATELLITE_CONFIG.items():
            
            # 4. Xác định khoảng thời gian (Date Logic)
            s_date, e_date = get_date_range(mode, params)
            
            # Xử lý đặc biệt cho mode HISTORICAL
            if mode == 'historical':
                s_date, e_date = get_satellite_dates(sat_config['id'])
                print(f"  ℹ️ [HISTORICAL] {sat_key}: {s_date} -> {e_date}")
            
            # Chuyển đổi string sang datetime để loop (nếu cần chia nhỏ)
            # Ở đây ta sẽ chia nhỏ theo THÁNG để tránh task quá lớn (Best Practice GEE)
            start_dt = datetime.strptime(s_date, '%Y-%m-%d')
            end_dt = datetime.strptime(e_date, '%Y-%m-%d')
            
            current_dt = start_dt
            while current_dt < end_dt:
                # Tính ngày cuối tháng hoặc end_date
                # Logic: Lấy ngày đầu tháng sau, rồi trừ 1 ngày -> cuối tháng này? 
                # Hoặc đơn giản: Loop từng tháng: 2000-01-01 -> 2000-02-01
                
                next_month = current_dt + timedelta(days=32)
                next_month = next_month.replace(day=1) # Ngày 1 tháng sau
                
                chunk_end_dt = min(next_month, end_dt)
                
                # Format lại thành string cho hàm export
                chunk_start_str = current_dt.strftime('%Y-%m-%d')
                chunk_end_str = chunk_end_dt.strftime('%Y-%m-%d')
                
                if chunk_start_str == chunk_end_str:
                    break
                
                # Gọi hàm export
                task_id = export_single_period(
                    city_name=city_name,
                    roi=roi,
                    collection_info=sat_config,
                    start_date=chunk_start_str,
                    end_date=chunk_end_str,
                    bucket_name=bucket_name,
                    base_folder_in_bucket=base_folder,
                    existing_files=existing_files
                )
                
                if task_id and task_id != "SKIPPED":
                    submitted_tasks.append(task_id)
                
                # Next loop
                current_dt = chunk_end_dt
                
    # 5. Trả về danh sách task ID
    print(f"📊 [SUMMARY] Đã gửi {len(submitted_tasks)} tasks lên GEE.")
    return submitted_tasks

def task_wait_completion(**kwargs):
    """
    Task Wait: Chờ task hoàn thành.
    """
    ti = kwargs['ti']
    submitted_tasks = ti.xcom_pull(task_ids='export_unified')
    wait_for_tasks(submitted_tasks)

def task_download_local(**kwargs):
    """
    Task Download: Tải file về máy.
    """
    run_download_pipeline()

# ==============================================================================
# 3. ĐỊNH NGHĨA OPERATORS & LUỒNG
# ==============================================================================

with dag:
    sensor = PythonSensor(
        task_id='check_gee_quota',
        python_callable=sensor_check_quota,
        mode='reschedule',
        poke_interval=600,
        timeout=3600 * 24
    )
    
    export = PythonOperator(
        task_id='export_unified',
        python_callable=task_export_unified,
        pool='gee_api_pool',
    )
    
    wait = PythonOperator(
        task_id='wait_for_completion',
        python_callable=task_wait_completion,
    )
    
    download = PythonOperator(
        task_id='download_to_local',
        python_callable=task_download_local,
    )
    
    sensor >> export >> wait >> download
