import os
from google.cloud import storage
from config.config import SERVICE_ACCOUNT_FILE, BASE_DIR

def check_bucket(bucket_name, prefix):
    """
    Quét toàn bộ Bucket tại prefix chỉ định và trả về một SET chứa các tên file.
    
    Args:
        bucket_name (str): Tên bucket.
        prefix (str): Đường dẫn thư mục gốc (VD: 'raw_data/hanoi/').
        
    Returns:
        set: Tập hợp các đường dẫn file (blob names) đã tồn tại.
    """
    print(f"--- Đang quét danh sách file trên gs://{bucket_name}/{prefix} ... ---")
    
    try:
        # 1. Kết nối
        client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
        bucket = client.bucket(bucket_name)
        
        # 2. Liệt kê (List blobs)
        # Google tự động xử lý phân trang (pagination) nếu list quá dài
        blobs = bucket.list_blobs(prefix=prefix)
        
        # 3. Lưu vào Set (Chỉ lấy tên file)
        existing_files = {blob.name for blob in blobs}
        
        print(f"  > Đã tìm thấy {len(existing_files)} file hiện có.")
        
        # 4. Ghi log ra file tạm để debug
        log_dir = os.path.join(BASE_DIR, 'logs')
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'gcs_scan_debug.txt')
        
        with open(log_file, 'w') as f:
            f.write(f"Scan time: {os.popen('date').read().strip()}\n")
            f.write(f"Bucket: {bucket_name}\n")
            f.write(f"Prefix: {prefix}\n")
            f.write(f"Total files: {len(existing_files)}\n")
            f.write("-" * 50 + "\n")
            for file in sorted(existing_files):
                f.write(f"{file}\n")
                
        print(f"  > Đã lưu log scan vào: {log_file}")
        
        return existing_files
        
    except Exception as e:
        print(f"  [ERROR] Lỗi khi quét bucket: {e}")
        return set() # Trả về set rỗng nếu lỗi để an toàn (coi như chưa có gì)
