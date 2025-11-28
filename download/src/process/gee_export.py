import ee
import os
from datetime import datetime, timezone, timedelta
from google.cloud import storage
from config.config import SERVICE_ACCOUNT_FILE

# Import các bộ lọc
from src.filters.lst_filters import apply_mask_lst_modis, apply_mask_lst_viirs
from src.filters.vi_filters import apply_mask_ndvi

def export_to_bucket(
    city_name,
    roi,
    collection_info,
    start_date,
    end_date,
    bucket_name,
    base_folder_in_bucket,
    existing_files=None, # <--- Thêm tham số này (Set)
    pending_tasks=None   # <--- [NEW] Thêm tham số này (Set)
):
    """
    Hàm xử lý và xuất 1 ảnh duy nhất cho 1 khoảng thời gian xác định.
    """
    if existing_files is None:
        existing_files = set()
    if pending_tasks is None:
        pending_tasks = set()

    # 1. Cấu hình tên file và đường dẫn (Load)
    # 1. Cấu hình tên file và đường dẫn (Load)
    col_id = collection_info['id']
    
    # Chuẩn hóa thông tin
    if isinstance(col_id, list):
        # Nếu là list (Composite), lấy tên từ config hoặc mặc định
        product_short_name = collection_info.get('product_name', 'COMPOSITE')
    else:
        product_short_name = col_id.split('/')[-1].upper()
    
    # Định dạng ngày gọn
    s_date = start_date.replace('-', '')
    e_date = end_date.replace('-', '')

    # Tạo tên file
    filename = f"{city_name}_{product_short_name}_{s_date}_{e_date}_mean"
    
    # Tạo đường dẫn cây thư mục
    gcs_path = f"{base_folder_in_bucket}/{city_name}/{product_short_name}/{filename}"
    
    # Tên file đầy đủ trên Bucket (GEE tự động thêm .tif)
    full_blob_name = f"{gcs_path}.tif"

    # -----------------------------------------------------------
    # [QUAN TRỌNG] BƯỚC KIỂM TRA TỒN TẠI (ACTUAL DATE LOGIC)
    # -----------------------------------------------------------
    # 1. Kiểm tra file "Mục tiêu" (Full Range)
    # Ví dụ: ..._20231101_20231130_mean.tif
    if full_blob_name in existing_files:
        print(f"  ⚡ [SKIP] File đầy đủ đã tồn tại: {full_blob_name}")
        
        # --- CLEANUP LOGIC ---
        # Tìm và xóa các file partial cũ (VD: ..._25)
        # prefix_pattern VD: .../Hanoi_MOD13Q1_20231101_
        # gcs_path VD: raw_data/hanoi/MOD13Q1/Hanoi_MOD13Q1_20231101_20231130_mean
        # Chúng ta cần lấy phần prefix chung: raw_data/hanoi/MOD13Q1/Hanoi_MOD13Q1_20231101_
        
        # Cách an toàn: Lấy tên file base (không có đuôi .tif)
        base_name_no_ext = gcs_path.split('/')[-1] # Hanoi_MOD13Q1_20231101_20231130_mean
        parts = base_name_no_ext.split('_')
        # parts: ['Hanoi', 'MOD13Q1', '20231101', '20231130', 'mean']
        
        # Reconstruct prefix: Hanoi_MOD13Q1_20231101_
        # Lưu ý: parts[:-2] sẽ lấy đến 20231101.
        # Cần ghép lại cẩn thận.
        
        # Cách đơn giản hơn: Cắt chuỗi từ gcs_path
        # Tìm vị trí của start_date trong chuỗi
        s_date_str = start_date.replace('-', '')
        try:
            # Tìm index của start_date
            idx = gcs_path.rfind(s_date_str)
            if idx != -1:
                # Prefix là từ đầu đến hết start_date + '_'
                # VD: .../Hanoi_MOD13Q1_20231101_
                cleanup_prefix = gcs_path[:idx + len(s_date_str) + 1]
                
                # Duyệt qua danh sách file hiện có để tìm file thừa
                for existing in list(existing_files):
                    # Chỉ xét các file .tif và bắt đầu bằng prefix này
                    if existing.startswith(cleanup_prefix) and existing.endswith('.tif') and existing != full_blob_name:
                        print(f"  🧹 [CLEANUP] Phát hiện file thừa (Partial): {existing}. Đang xóa...")
                        try:
                            client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
                            bucket = client.bucket(bucket_name)
                            blob = bucket.blob(existing)
                            blob.delete()
                            print(f"    ✅ Đã xóa: {existing}")
                        except Exception as e:
                            print(f"    ❌ Lỗi khi xóa {existing}: {e}")
        except Exception as e:
            print(f"  ⚠️ [WARN] Lỗi logic cleanup: {e}")
        # ---------------------
        
        return "SKIPPED"
    
    # [NEW] Check Pending Tasks (Full Month)
    # Nếu task export file này đang chạy -> Skip
    if filename in pending_tasks:
        print(f"  ⏳ [PENDING] Task đang chạy trên GEE: {filename}")
        return "SKIPPED"
    
    # 2. Nếu chưa có file Full, chuẩn bị query GEE để lấy ngày thực tế
    # (Logic này sẽ được thực hiện bên dưới, sau khi tạo ImageCollection)
    # -----------------------------------------------------------

    # 2. Lấy thông tin cấu hình xử lý ảnh
    prod_type = collection_info['type']
    data_bands = collection_info['data_bands']
    qc_band = collection_info['qc_band']
    scale = collection_info.get('scale', 1000)

    # 3. Router Logic
    def apply_filters(img):
        # Lấy ID của ảnh để biết nó thuộc MOD hay MYD (quan trọng khi merge)
        img_id = img.get('system:index') # Hoặc check metadata khác nếu cần
        
        # Lưu ý: Khi merge, col_id ở ngoài là list, nên logic check ID bên trong cần linh hoạt
        # Tuy nhiên, các hàm filter hiện tại check dựa trên 'col_id' biến global (closure).
        # Cần sửa logic này nếu col_id là list.
        
        # Tạm thời: Với VI, logic MOD và MYD giống hệt nhau (DetailedQA), nên không cần phân biệt quá kỹ.
        if prod_type == 'LST':
            # Logic LST cần phân biệt MOD/MYD/VNP
            # Nhưng hiện tại LST chưa gộp, nên col_id vẫn là string.
            if 'MODIS' in str(col_id) or 'MYD' in str(col_id) or 'MOD' in str(col_id):
                return apply_mask_lst_modis(img, data_bands, qc_band)
            elif 'VIIRS' in str(col_id) or 'VNP' in str(col_id):
                return apply_mask_lst_viirs(img, data_bands, qc_band)
        elif prod_type == 'VI':
            return apply_mask_ndvi(img, data_bands, qc_band)
        return img

    # 4. Lấy dữ liệu, Xử lý và Export
    try:
        if isinstance(col_id, list):
            # Merge Collections
            col = ee.ImageCollection(col_id[0])
            for cid in col_id[1:]:
                col = col.merge(ee.ImageCollection(cid))
        else:
            col = ee.ImageCollection(col_id)
            
        col = col.filterDate(start_date, end_date).filterBounds(roi)
        
        count = col.limit(1).size().getInfo()
        if count == 0:
            print(f"  [SKIP] Không có ảnh nào từ {start_date} đến {end_date} cho {city_name}")
            return None

        # --- ACTUAL DATE LOGIC ---
        # Lấy ngày cuối cùng thực tế có dữ liệu trong khoảng thời gian này
        # Ví dụ: Yêu cầu đến 30/11 nhưng dữ liệu mới có đến 25/11
        actual_last_img = col.sort('system:time_start', False).first()
        actual_end_date_ms = actual_last_img.get('system:time_start').getInfo()
        actual_end_date = datetime.fromtimestamp(actual_end_date_ms / 1000).strftime('%Y-%m-%d')
        
        # [NEW] Rounding Logic (Align with User Convention)
        # Nếu ngày thực tế gần với ngày yêu cầu (cách < 2 ngày) -> Dùng ngày yêu cầu (Full Month)
        # VD: Actual = 31/01, Requested = 01/02 -> Diff = 1 day -> Use 01/02
        actual_end_dt = datetime.strptime(actual_end_date, '%Y-%m-%d')
        requested_end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        # [STRICT] Chỉ làm tròn nếu dữ liệu thực sự chạm đến ngày cuối cùng của tháng (cách 1 ngày so với ngày 1 tháng sau)
        # VD: Actual = 30/11, Requested = 01/12 -> Diff = 1 day -> Round UP (Full)
        # Get tolerance from config, default to 1 day (for daily products)
        tolerance_days = collection_info.get('date_tolerance_days', 1)
        force_historical_full = collection_info.get('force_historical_full', False)
        
        # Check if the requested period is "historical" (strictly before the current month)
        # We compare requested_end_dt with the start of the current month
        current_month_start = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        is_historical = requested_end_dt < current_month_start

        if force_historical_full and is_historical:
             final_end_date_str = end_date
             print(f"  ℹ️ [INFO] Historical Month (Forced Full): Using requested end date {end_date}")
        elif (requested_end_dt - actual_end_dt).days <= tolerance_days:
             final_end_date_str = end_date # Dùng ngày yêu cầu (VD: 20250901)
             print(f"  ℹ️ [INFO] Data is complete ({actual_end_date}). Using requested end date: {end_date}")
        else:
             final_end_date_str = actual_end_date # Dùng ngày thực tế (VD: 20250825)
             print(f"  ⚠️ [INFO] Data is partial. Actual end: {actual_end_date} (Requested: {end_date})")

        # Tạo tên file thực tế
        a_e_date = final_end_date_str.replace('-', '')
        actual_filename = f"{city_name}_{product_short_name}_{s_date}_{a_e_date}_mean"
        actual_gcs_path = f"{base_folder_in_bucket}/{city_name}/{product_short_name}/{actual_filename}"
        actual_full_blob_name = f"{actual_gcs_path}.tif"
        
        # Kiểm tra lại: Nếu file thực tế này đã có rồi -> Skip
        if actual_full_blob_name in existing_files:
             print(f"  ⚡ [SKIP] File thực tế đã tồn tại: {actual_full_blob_name}")
             return "SKIPPED"
             
        # [NEW] Check Pending Tasks (Actual File)
        if actual_filename in pending_tasks:
             print(f"  ⏳ [PENDING] Task thực tế đang chạy trên GEE: {actual_filename}")
             return "SKIPPED"

        print(f"  ℹ️ [INFO] Exporting Range: {start_date} -> {final_end_date_str}")
        # -------------------------

        processed_img = col.map(apply_filters).mean()
        final_image = processed_img.clip(roi)
        
        task = ee.batch.Export.image.toCloudStorage(
            image=final_image,
            description=actual_filename, # Dùng tên file thực tế
            bucket=bucket_name,
            fileNamePrefix=actual_gcs_path, # Dùng đường dẫn thực tế
            region=roi.geometry(),
            scale=scale,
            crs='EPSG:4326',
            fileFormat='GeoTIFF',
            maxPixels=1e13
        )

        task.start()
        print(f"  ✅ [SUBMITTED] {filename} (ID: {task.id})")
        return task.id

    except Exception as e:
        print(f"  ❌ [FAIL] Lỗi xử lý/export cho {start_date}: {e}")
        return None