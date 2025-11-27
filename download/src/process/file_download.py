import os
import concurrent.futures
from google.cloud import storage
from config.config import GCS_CONFIG, LOCAL_DATA_DIR
from src.auth import get_gcs_credentials

def download_blob(bucket_name, blob_name, destination_file_name, storage_client):
    """
    Hàm worker: Tải 1 file từ GCS về Local.
    Nhận storage_client từ bên ngoài để tận dụng connection pooling.
    """
    try:
        # Tạo thư mục cha nếu chưa có
        os.makedirs(os.path.dirname(destination_file_name), exist_ok=True)
        
        # Sử dụng client được truyền vào (Thread-safe)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        # Tải về
        blob.download_to_filename(destination_file_name)
        print(f"  ✅ [OK] Downloaded: {blob_name}")
        return True
    except Exception as e:
        print(f"  ❌ [FAIL] Error downloading {blob_name}: {e}")
        return False

def run_download_pipeline():
    """
    Quy trình chính: Scan GCS -> Check Local -> Download Delta.
    """
    bucket_name = GCS_CONFIG['bucket_name']
    base_folder = GCS_CONFIG['base_folder']
    max_workers = GCS_CONFIG['download_threads']
    
    print(f"🚀 [START] Bắt đầu quy trình đồng bộ dữ liệu từ gs://{bucket_name}/{base_folder}")
    
    try:
        # 1. Xác thực & Khởi tạo Client (1 lần duy nhất)
        credentials = get_gcs_credentials()
        storage_client = storage.Client(credentials=credentials)
        
        # 2. Quét file trên Cloud
        print("--- 1. Scanning Cloud Storage... ---")
        blobs = storage_client.list_blobs(bucket_name, prefix=base_folder)
        
        download_queue = []
        
        for blob in blobs:
            if blob.name.endswith("/"): # Bỏ qua thư mục ảo
                continue
                
            relative_path = blob.name
            local_path = os.path.join(LOCAL_DATA_DIR, relative_path)
            
            # 3. Kiểm tra tồn tại (Delta Sync)
            if os.path.exists(local_path):
                continue
            else:
                download_queue.append((blob.name, local_path))
                
        print(f"  > Tìm thấy {len(download_queue)} file mới cần tải.")
        
        if not download_queue:
            print("🎉 [DONE] Dữ liệu đã đồng bộ hoàn toàn. Không cần tải thêm.")
            return

        # 4. Tải song song (Multi-threading)
        print(f"--- 2. Downloading with {max_workers} threads... ---")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks: Truyền storage_client vào worker
            futures = [
                executor.submit(download_blob, bucket_name, blob_name, local_path, storage_client)
                for blob_name, local_path in download_queue
            ]
            
            # Wait for completion
            concurrent.futures.wait(futures)
            
        print("🎉 [DONE] Hoàn tất quá trình tải về.")

    except Exception as e:
        print(f"❌ [CRITICAL] Lỗi luồng download: {e}")

if __name__ == "__main__":
    run_download_pipeline()
