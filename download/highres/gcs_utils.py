import os
import concurrent.futures
from google.cloud import storage
from config.config import GCS_CONFIG
from highres.config import SERVICE_ACCOUNT_FILE, HIGHRES_LOCAL_DIR, HIGHRES_GCS_FOLDER

def get_gcs_client():
    """
    Trả về client GCS đã xác thực.
    """
    return storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)

def scan_highres_gcs_files():
    """
    Quét danh sách các file ảnh highres đã xuất trên GCS.
    """
    bucket_name = GCS_CONFIG['bucket_name']
    # Prefix dạng: highres/
    prefix = f"{HIGHRES_GCS_FOLDER}/"
    print(f"--- [HIGHRES-SCAN] Quét danh sách file trên GCS: gs://{bucket_name}/{prefix} ---")
    
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)
        
        existing_files = {blob.name for blob in blobs if not blob.name.endswith("/")}
        print(f"  > Tìm thấy {len(existing_files)} file highres trên GCS.")
        return existing_files
    except Exception as e:
        print(f"  ❌ [HIGHRES-SCAN] Lỗi khi quét bucket: {e}")
        return set()

def download_single_blob(bucket_name, blob_name, dest_path, storage_client):
    """
    Worker download một file từ GCS về local.
    """
    try:
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        blob.download_to_filename(dest_path)
        print(f"  ✅ [DOWNLOAD] Tải thành công: {blob_name} -> {dest_path}")
        return True
    except Exception as e:
        print(f"  ❌ [DOWNLOAD] Lỗi khi tải {blob_name}: {e}")
        return False

def sync_highres_to_local():
    """
    Đồng bộ delta dữ liệu highres từ GCS về thư mục local của máy host.
    Đồng thời dọn dẹp các tệp tin local rác không tồn tại trên Cloud.
    """
    bucket_name = GCS_CONFIG['bucket_name']
    base_folder = GCS_CONFIG['base_folder']
    max_workers = GCS_CONFIG.get('download_threads', 16)
    
    print("🚀 [START-SYNC] Khởi động đồng bộ dữ liệu Highres...")
    
    try:
        client = get_gcs_client()
        
        # 1. Quét file trên GCS
        cloud_files = scan_highres_gcs_files()
        
        # 2. Lập hàng đợi download (Delta Sync)
        download_queue = []
        for blob_name in cloud_files:
            # blob_name dạng: highres/hanoi/LC08/hanoi_LC08_20190113.tif
            # File local đích
            local_path = os.path.join(HIGHRES_LOCAL_DIR, os.path.relpath(blob_name, HIGHRES_GCS_FOLDER))
            
            if not os.path.exists(local_path):
                download_queue.append((blob_name, local_path))
                
        print(f"  > Tìm thấy {len(download_queue)} file mới cần tải về.")
        
        # 3. Dọn dẹp local thừa (Mirror Sync)
        print("--- Dọn dẹp tệp tin rác tại Local... ---")
        if os.path.exists(HIGHRES_LOCAL_DIR):
            for root, dirs, files in os.walk(HIGHRES_LOCAL_DIR):
                for file in files:
                    if file.startswith("."): 
                        continue
                    abs_path = os.path.join(root, file)
                    
                    # Tính relative path tương ứng với GCS
                    rel_to_highres = os.path.relpath(abs_path, HIGHRES_LOCAL_DIR)
                    gcs_rel_path = f"{HIGHRES_GCS_FOLDER}/{rel_to_highres}".replace(os.sep, '/')
                    
                    if gcs_rel_path not in cloud_files:
                        print(f"  🧹 [CLEANUP] Xóa file local không còn trên GCS: {rel_to_highres}")
                        try:
                            os.remove(abs_path)
                        except Exception as e:
                            print(f"    ❌ Lỗi xóa file: {e}")
                            
        # 4. Tải song song
        if download_queue:
            print(f"--- Đang tải về sử dụng {max_workers} luồng... ---")
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(download_single_blob, bucket_name, blob_name, local_path, client)
                    for blob_name, local_path in download_queue
                ]
                concurrent.futures.wait(futures)
            print("🎉 [FINISH-SYNC] Hoàn tất quá trình đồng bộ dữ liệu Highres về local.")
        else:
            print("🎉 [FINISH-SYNC] Dữ liệu đã đồng bộ hoàn toàn, không cần tải thêm.")
            
    except Exception as e:
        print(f"❌ [CRITICAL-SYNC] Lỗi trong luồng đồng bộ: {e}")
