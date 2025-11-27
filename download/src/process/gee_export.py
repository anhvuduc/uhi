import ee
import os
# from google.cloud import storage # Không cần nữa vì check qua Set
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
    existing_files=None # <--- Thêm tham số này (Set)
):
    """
    Hàm xử lý và xuất 1 ảnh duy nhất cho 1 khoảng thời gian xác định.
    """
    if existing_files is None:
        existing_files = set()

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
    # [QUAN TRỌNG] BƯỚC KIỂM TRA TỒN TẠI (OPTIMIZED)
    # -----------------------------------------------------------
    # Kiểm tra trong Set (O(1)) thay vì gọi API (O(N))
    if full_blob_name in existing_files:
        print(f"  ⚡ [SKIP] File đã tồn tại: {full_blob_name}")
        return "SKIPPED"
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

        processed_img = col.map(apply_filters).mean()
        final_image = processed_img.clip(roi)
        
        task = ee.batch.Export.image.toCloudStorage(
            image=final_image,
            description=filename,
            bucket=bucket_name,
            fileNamePrefix=gcs_path,
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