from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from datetime import datetime, timedelta
import sys
import os
import time
import csv

# Thêm đường dẫn gốc vào Python path để Airflow tìm thấy các module
airflow_home = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
sys.path.append(airflow_home)

import ee
from src.auth import initialize_gee
from src.utils.gee_quota import check_gee_quota, get_pending_tasks
from src.utils.gee_coordinator import wait_for_tasks
from highres.config import ROIS, GCS_CONFIG, HIGHRES_GCS_FOLDER, GEE_MAX_ACTIVE_TASKS, LOCAL_DATA_DIR
from highres.landsat_process import get_landsat_available_dates, process_landsat_for_date
from highres.lst_process import process_lst_for_date_window
from highres.vi_process import process_vi_for_date_window
from highres.gcs_utils import sync_highres_to_local, scan_highres_gcs_files

# ==============================================================================
# 1. CẤU HÌNH DAG & PARAMS ĐỘNG
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
    'highres_download',
    default_args=default_args,
    description='Pipeline tải LST & NDVI độ phân giải cao Landsat-MODIS-VIIRS',
    schedule_interval=None,  # Chạy thủ công
    catchup=False,
    tags=['highres', 'landsat', 'modis', 'viirs', 'lst', 'vi', 'v2'],
    params={
        'date_source': Param('gee', type='string', description="Nguồn quét ngày. Chọn 'gee' để quét ngày Landsat sạch mây từ Earth Engine, hoặc 'local' để quét từ các file Landsat có sẵn tại thư mục local."),
        'highres_dir': Param('/opt/airflow/data/highres', type='string', description="Đường dẫn thư mục Landsat local để quét ngày (chỉ dùng khi date_source là 'local')"),
        'start_date': Param('2013-03-18', type='string', format='date', description="Ngày bắt đầu quét Landsat (chỉ dùng khi date_source là 'gee')"),
        'end_date': Param('2022-06-30', type='string', format='date', description="Ngày kết thúc quét Landsat (chỉ dùng khi date_source là 'gee')"),
        'cities': Param('hanoi,haiphong,danang,hcm', type='string', description="Danh sách các thành phố chạy (ngăn cách bởi dấu phẩy, VD: 'hanoi' hoặc 'hanoi,hcm'). Nhập 'all' để chạy tất cả"),
        'export_targets': Param('all', type='string', description="Nguồn dữ liệu cần xuất (ngăn cách bởi dấu phẩy, VD: 'landsat,modis_lst,modis_vi,viirs_lst,viirs_vi'). Nhập 'all' để xuất tất cả"),
    }
)

# ==============================================================================
# 2. CẤU HÌNH CÁC TASK CALLABLE
# ==============================================================================

def find_landsat_dates_callable(**kwargs):
    """
    Xác định danh sách ngày của từng thành phố dựa trên cấu hình date_source.
    """
    params = kwargs['params']
    date_source = params.get('date_source', 'gee')
    cities_str = params.get('cities', 'hanoi,haiphong,danang,hcm')
    
    # Phân tích danh sách các thành phố
    if not cities_str or cities_str.strip().lower() == 'all':
        cities = list(ROIS.keys())
    else:
        cities = [c.strip().lower() for c in cities_str.split(',') if c.strip()]
        
    city_dates_map = {}
    
    if date_source == 'local':
        import glob
        import re
        highres_dir = params.get('highres_dir', '/opt/airflow/data/highres')
        print(f"📂 [LOCAL-SCAN] Quét danh sách ngày từ thư mục Landsat local: {highres_dir}")
        
        for city in cities:
            city_dates_map[city] = set()
            for sat in ['LC08', 'LC09']:
                dir_path = os.path.join(highres_dir, city, sat)
                if not os.path.exists(dir_path):
                    continue
                pattern = os.path.join(dir_path, f"{city}_{sat}_*.tif")
                tif_files = glob.glob(pattern)
                for fpath in tif_files:
                    filename = os.path.basename(fpath)
                    if filename.endswith(".aux.xml"):
                        continue
                    # Tên file dạng: hanoi_LC08_20130929.tif
                    match = re.match(rf"{city}_{sat}_(\d{{8}})\.tif", filename)
                    if match:
                        date_raw = match.group(1)
                        date_str = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:]}"
                        city_dates_map[city].add(date_str)
            city_dates_map[city] = sorted(list(city_dates_map[city]))
            print(f"  > Thành phố {city.upper()}: Tìm thấy {len(city_dates_map[city])} ngày từ Landsat local.")
            
    else:
        # Earth Engine GEE Scan Mode
        start_date = params['start_date']
        end_date = params['end_date']
        
        initialize_gee()
        for city in cities:
            if city not in ROIS:
                print(f"⚠️ [WARN] Thành phố {city} không có trong cấu hình ROIS. Bỏ qua.")
                continue
                
            print(f"🚀 [GEE-SCAN] Đang quét ngày trên GEE cho {city} từ {start_date} đến {end_date}...")
            roi = ee.FeatureCollection(ROIS[city])
            try:
                dates = get_landsat_available_dates(roi, start_date, end_date)
                city_dates_map[city] = dates
            except Exception as e:
                print(f"❌ [GEE-SCAN] Lỗi khi quét ngày cho {city}: {e}")
                city_dates_map[city] = []
                
    return city_dates_map

def export_highres_callable(**kwargs):
    """
    Gửi lệnh xuất dữ liệu cho các vệ tinh lên GEE.
    """
    ti = kwargs['ti']
    city_dates_map = ti.xcom_pull(task_ids='find_landsat_dates')
    params = kwargs['params']
    date_source = params.get('date_source', 'gee')
    
    # Lấy các tham số lọc cấu hình động
    export_targets_str = params.get('export_targets', 'all')
    if not export_targets_str or export_targets_str.strip().lower() == 'all':
        enabled_targets = {'landsat', 'modis_lst', 'modis_vi', 'viirs_lst', 'viirs_vi'}
    else:
        enabled_targets = {t.strip().lower() for t in export_targets_str.split(',') if t.strip()}
        
    print(f"⚙️ [CONFIG] Các nguồn xuất: {enabled_targets}")
    
    initialize_gee()
    bucket_name = GCS_CONFIG['bucket_name']
    
    # Quét GCS và các task pending để tránh trùng
    existing_files = scan_highres_gcs_files()
    pending_tasks = get_pending_tasks()
    
    submitted_tasks = []
    
    # Header cho file log đối sánh
    log_fields = [
        'Date', 'City', 'Landsat_Exported', 'Landsat_Found', 'Landsat_Collection', 
        'Landsat_Cloud_Cover', 'Medium_Aerosol_Pixel_Ratio', 'MODIS_Terra_LST_Exported',
        'MODIS_Aqua_LST_Exported', 'MODIS_VI_Exported', 'VIIRS_LST_Exported', 'VIIRS_VI_Exported'
    ]
    
    log_dir = os.path.join(LOCAL_DATA_DIR, 'highres_logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file_path = os.path.join(log_dir, 'highres_export_match_log.csv')
    log_exists = os.path.exists(log_file_path)
    
    # Hàm kiểm soát quota task
    def wait_if_quota_full():
        while True:
            current_tasks = check_gee_quota()
            if current_tasks < GEE_MAX_ACTIVE_TASKS:
                break
            print(f"⏳ [QUOTA FULL] {current_tasks}/{GEE_MAX_ACTIVE_TASKS} tasks running. Chờ 2 phút...")
            time.sleep(120)
            
    for city_name, dates in city_dates_map.items():
        if not dates:
            print(f"📭 [EXPORT] {city_name} không có ngày nào cần xử lý.")
            continue
            
        roi = ee.FeatureCollection(ROIS[city_name])
        
        for date_str in dates:
            print(f"\n--- 📦 ĐANG XỬ LÝ NGÀY: {date_str} CHO THÀNH PHỐ: {city_name.upper()} ---")
            
            landsat_exported = "SKIPPED"
            landsat_found = False
            landsat_coll = "N/A"
            landsat_cc = "N/A"
            aerosol_ratio = "N/A"
            mod_lst_exported = "SKIPPED"
            myd_lst_exported = "SKIPPED"
            mxd_vi_exported = "SKIPPED"
            vnp_lst_exported = "SKIPPED"
            vnp_vi_exported = "SKIPPED"
            
            # A. Kiểm tra và xử lý Landsat trước
            lst_landsat_img = None
            landsat_meta = None
            try:
                lst_landsat_img, landsat_meta = process_landsat_for_date(roi, date_str)
                if lst_landsat_img is not None:
                    landsat_found = True
                    landsat_coll = landsat_meta['collection']
                    landsat_cc = landsat_meta['cloud_cover']
                    aerosol_ratio = landsat_meta['medium_aerosol_pixel_ratio']
            except Exception as e:
                print(f"  ❌ [LANDSAT-CHECK] Lỗi xử lý/truy vấn từ GEE: {e}")
                
            # Nếu ở chế độ local và không cần xuất Landsat, ta có thể dùng fallback thay vì bỏ qua nếu GEE lỗi
            if not landsat_found and date_source == 'local':
                landsat_found = True
                landsat_coll = 'LC08'
                print(f"  ℹ️ [LOCAL-FALLBACK] Không check được Landsat từ GEE, sử dụng fallback cho chế độ local.")
                
            # Nếu vẫn không tìm thấy ảnh Landsat hợp lệ, ta bỏ qua ngày này
            if not landsat_found:
                print(f"  ⚡ [SKIP] Bỏ qua ngày {date_str} cho {city_name} vì không thể trích xuất ảnh Landsat sạch mây.")
                continue
            
            # 1. Landsat 8/9 Export
            if 'landsat' in enabled_targets and landsat_found and lst_landsat_img is not None:
                l_filename = f"{city_name}_{landsat_coll}_{date_str.replace('-', '')}"
                l_gcs_path = f"{HIGHRES_GCS_FOLDER}/{city_name}/{landsat_coll}/{l_filename}"
                l_full_blob = f"{l_gcs_path}.tif"
                
                if l_full_blob not in existing_files and l_filename not in pending_tasks:
                    try:
                        wait_if_quota_full()
                        task = ee.batch.Export.image.toCloudStorage(
                            image=lst_landsat_img,
                            description=l_filename,
                            bucket=bucket_name,
                            fileNamePrefix=l_gcs_path,
                            region=roi.geometry(),
                            scale=100,
                            crs='EPSG:4326',
                            fileFormat='GeoTIFF',
                            maxPixels=1e13
                        )
                        task.start()
                        submitted_tasks.append(task.id)
                        landsat_exported = "SUBMITTED"
                        print(f"  ✅ [LANDSAT] Gửi lệnh Export: {l_filename} (ID: {task.id})")
                    except Exception as e:
                        landsat_exported = f"ERROR: {e}"
                        print(f"  ❌ [LANDSAT] Lỗi Export: {e}")
                else:
                    landsat_exported = "EXISTING" if l_full_blob in existing_files else "PENDING"
                    print(f"  ⚡ [LANDSAT] Đã tồn tại/Đang chạy: {l_filename}")
                
            # 2. MODIS Daily LST (Terra + Aqua)
            if 'modis_lst' in enabled_targets:
                # MODIS Terra (MOD21A1D)
                mod_filename = f"{city_name}_MOD21A1D_{date_str.replace('-', '')}"
                mod_gcs_path = f"{HIGHRES_GCS_FOLDER}/{city_name}/MOD21A1D/{mod_filename}"
                mod_full_blob = f"{mod_gcs_path}.tif"
                if mod_full_blob not in existing_files and mod_filename not in pending_tasks:
                    try:
                        wait_if_quota_full()
                        mod_img = process_lst_for_date_window(roi, date_str, 'MOD21A1D')
                        if mod_img is not None:
                            task = ee.batch.Export.image.toCloudStorage(
                                image=mod_img,
                                description=mod_filename,
                                bucket=bucket_name,
                                fileNamePrefix=mod_gcs_path,
                                region=roi.geometry(),
                                scale=100,
                                crs='EPSG:4326',
                                fileFormat='GeoTIFF',
                                maxPixels=1e13
                            )
                            task.start()
                            submitted_tasks.append(task.id)
                            mod_lst_exported = "SUBMITTED"
                            print(f"  ✅ [MODIS-TERRA-LST] Gửi lệnh Export: {mod_filename} (ID: {task.id})")
                    except Exception as e:
                        mod_lst_exported = f"ERROR: {e}"
                        print(f"  ❌ [MODIS-TERRA-LST] Lỗi: {e}")
                else:
                    mod_lst_exported = "EXISTING" if mod_full_blob in existing_files else "PENDING"
                    print(f"  ⚡ [MODIS-TERRA-LST] Đã tồn tại/Đang chạy: {mod_filename}")
                    
                # MODIS Aqua (MYD21A1D)
                myd_filename = f"{city_name}_MYD21A1D_{date_str.replace('-', '')}"
                myd_gcs_path = f"{HIGHRES_GCS_FOLDER}/{city_name}/MYD21A1D/{myd_filename}"
                myd_full_blob = f"{myd_gcs_path}.tif"
                if myd_full_blob not in existing_files and myd_filename not in pending_tasks:
                    try:
                        wait_if_quota_full()
                        myd_img = process_lst_for_date_window(roi, date_str, 'MYD21A1D')
                        if myd_img is not None:
                            task = ee.batch.Export.image.toCloudStorage(
                                image=myd_img,
                                description=myd_filename,
                                bucket=bucket_name,
                                fileNamePrefix=myd_gcs_path,
                                region=roi.geometry(),
                                scale=100,
                                crs='EPSG:4326',
                                fileFormat='GeoTIFF',
                                maxPixels=1e13
                            )
                            task.start()
                            submitted_tasks.append(task.id)
                            myd_lst_exported = "SUBMITTED"
                            print(f"  ✅ [MODIS-AQUA-LST] Gửi lệnh Export: {myd_filename} (ID: {task.id})")
                    except Exception as e:
                        myd_lst_exported = f"ERROR: {e}"
                        print(f"  ❌ [MODIS-AQUA-LST] Lỗi: {e}")
                else:
                    myd_lst_exported = "EXISTING" if myd_full_blob in existing_files else "PENDING"
                    print(f"  ⚡ [MODIS-AQUA-LST] Đã tồn tại/Đang chạy: {myd_filename}")
                    
            # 3. MODIS Vegetation Indices (MXD13A1 - Terra + Aqua)
            if 'modis_vi' in enabled_targets:
                mxd_filename = f"{city_name}_MXD13A1_{date_str.replace('-', '')}"
                mxd_gcs_path = f"{HIGHRES_GCS_FOLDER}/{city_name}/MXD13A1/{mxd_filename}"
                mxd_full_blob = f"{mxd_gcs_path}.tif"
                if mxd_full_blob not in existing_files and mxd_filename not in pending_tasks:
                    try:
                        wait_if_quota_full()
                        mxd_img = process_vi_for_date_window(roi, date_str, 'MXD13A1')
                        if mxd_img is not None:
                            task = ee.batch.Export.image.toCloudStorage(
                                image=mxd_img,
                                description=mxd_filename,
                                bucket=bucket_name,
                                fileNamePrefix=mxd_gcs_path,
                                region=roi.geometry(),
                                scale=100,
                                crs='EPSG:4326',
                                fileFormat='GeoTIFF',
                                maxPixels=1e13
                            )
                            task.start()
                            submitted_tasks.append(task.id)
                            mxd_vi_exported = "SUBMITTED"
                            print(f"  ✅ [MODIS-VI] Gửi lệnh Export: {mxd_filename} (ID: {task.id})")
                    except Exception as e:
                        mxd_vi_exported = f"ERROR: {e}"
                        print(f"  ❌ [MODIS-VI] Lỗi: {e}")
                else:
                    mxd_vi_exported = "EXISTING" if mxd_full_blob in existing_files else "PENDING"
                    print(f"  ⚡ [MODIS-VI] Đã tồn tại/Đang chạy: {mxd_filename}")
                    
            # 4. VIIRS SNPP Daily LST (VNP21A1D)
            if 'viirs_lst' in enabled_targets:
                vnp_filename = f"{city_name}_VNP21A1D_{date_str.replace('-', '')}"
                vnp_gcs_path = f"{HIGHRES_GCS_FOLDER}/{city_name}/VNP21A1D/{vnp_filename}"
                vnp_full_blob = f"{vnp_gcs_path}.tif"
                if vnp_full_blob not in existing_files and vnp_filename not in pending_tasks:
                    try:
                        wait_if_quota_full()
                        vnp_img = process_lst_for_date_window(roi, date_str, 'VNP21A1D')
                        if vnp_img is not None:
                            task = ee.batch.Export.image.toCloudStorage(
                                image=vnp_img,
                                description=vnp_filename,
                                bucket=bucket_name,
                                fileNamePrefix=vnp_gcs_path,
                                region=roi.geometry(),
                                scale=100,
                                crs='EPSG:4326',
                                fileFormat='GeoTIFF',
                                maxPixels=1e13
                            )
                            task.start()
                            submitted_tasks.append(task.id)
                            vnp_lst_exported = "SUBMITTED"
                            print(f"  ✅ [VIIRS-LST] Gửi lệnh Export: {vnp_filename} (ID: {task.id})")
                    except Exception as e:
                        vnp_lst_exported = f"ERROR: {e}"
                        print(f"  ❌ [VIIRS-LST] Lỗi: {e}")
                else:
                    vnp_lst_exported = "EXISTING" if vnp_full_blob in existing_files else "PENDING"
                    print(f"  ⚡ [VIIRS-LST] Đã tồn tại/Đang chạy: {vnp_filename}")
                    
            # 5. VIIRS Vegetation Indices (VNP13A1)
            if 'viirs_vi' in enabled_targets:
                vnp_vi_filename = f"{city_name}_VNP13A1_{date_str.replace('-', '')}"
                vnp_vi_gcs_path = f"{HIGHRES_GCS_FOLDER}/{city_name}/VNP13A1/{vnp_vi_filename}"
                vnp_vi_full_blob = f"{vnp_vi_gcs_path}.tif"
                if vnp_vi_full_blob not in existing_files and vnp_vi_filename not in pending_tasks:
                    try:
                        wait_if_quota_full()
                        vnp_vi_img = process_vi_for_date_window(roi, date_str, 'VNP13A1')
                        if vnp_vi_img is not None:
                            task = ee.batch.Export.image.toCloudStorage(
                                image=vnp_vi_img,
                                description=vnp_vi_filename,
                                bucket=bucket_name,
                                fileNamePrefix=vnp_vi_gcs_path,
                                region=roi.geometry(),
                                scale=100,
                                crs='EPSG:4326',
                                fileFormat='GeoTIFF',
                                maxPixels=1e13
                            )
                            task.start()
                            submitted_tasks.append(task.id)
                            vnp_vi_exported = "SUBMITTED"
                            print(f"  ✅ [VIIRS-VI] Gửi lệnh Export: {vnp_vi_filename} (ID: {task.id})")
                    except Exception as e:
                        vnp_vi_exported = f"ERROR: {e}"
                        print(f"  ❌ [VIIRS-VI] Lỗi: {e}")
                else:
                    vnp_vi_exported = "EXISTING" if vnp_vi_full_blob in existing_files else "PENDING"
                    print(f"  ⚡ [VIIRS-VI] Đã tồn tại/Đang chạy: {vnp_vi_filename}")
                    
            # Tạo log entry và ghi log
            log_entry = {
                'Date': date_str,
                'City': city_name,
                'Landsat_Exported': landsat_exported,
                'Landsat_Found': landsat_found,
                'Landsat_Collection': landsat_coll,
                'Landsat_Cloud_Cover': landsat_cc,
                'Medium_Aerosol_Pixel_Ratio': aerosol_ratio,
                'MODIS_Terra_LST_Exported': mod_lst_exported,
                'MODIS_Aqua_LST_Exported': myd_lst_exported,
                'MODIS_VI_Exported': mxd_vi_exported,
                'VIIRS_LST_Exported': vnp_lst_exported,
                'VIIRS_VI_Exported': vnp_vi_exported
            }
            
            with open(log_file_path, mode='a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=log_fields)
                if not log_exists:
                    writer.writeheader()
                    log_exists = True
                writer.writerow(log_entry)
                
    print(f"📊 [SUMMARY] Gửi thành công {len(submitted_tasks)} task lên GEE.")
    print(f"📋 [LOG] Bản ghi đối sánh chi tiết đã lưu tại: {log_file_path}")
    return submitted_tasks

def wait_gee_tasks_callable(**kwargs):
    """
    Đợi toàn bộ các tác vụ vừa gửi lên GEE hoàn tất.
    """
    ti = kwargs['ti']
    submitted_tasks = ti.xcom_pull(task_ids='export_highres')
    wait_for_tasks(submitted_tasks)
    
    # Kiểm tra an toàn: đợi cho đến khi hệ thống GEE rảnh hoàn toàn (0 task READY/RUNNING)
    print("⏳ [WAIT] Đang đợi hệ thống GEE xử lý sạch hàng đợi...")
    while True:
        count = check_gee_quota()
        if count == 0:
            print("✅ [GEE-DONE] Toàn bộ tác vụ đã kết thúc.")
            break
        print(f"    ... Vẫn còn {count} tác vụ đang chạy trên GEE. Chờ 60s...")
        time.sleep(60)

def download_highres_local_callable(**kwargs):
    """
    Tải dữ liệu từ GCS về Local.
    """
    sync_highres_to_local()

# ==============================================================================
# 3. ĐỊNH NGHĨA CÁC TASK & LUỒNG XỬ LÝ (PIPELINE DAG)
# ==============================================================================

with dag:
    find_landsat_dates = PythonOperator(
        task_id='find_landsat_dates',
        python_callable=find_landsat_dates_callable,
    )
    
    export_highres = PythonOperator(
        task_id='export_highres',
        python_callable=export_highres_callable,
    )
    
    wait_gee_tasks = PythonOperator(
        task_id='wait_gee_tasks',
        python_callable=wait_gee_tasks_callable,
    )
    
    download_highres_local = PythonOperator(
        task_id='download_highres_local',
        python_callable=download_highres_local_callable,
    )
    
    # Luồng xử lý của DAG
    find_landsat_dates >> export_highres >> wait_gee_tasks >> download_highres_local
