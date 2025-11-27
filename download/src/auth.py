import ee
import os
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from config.config import SERVICE_ACCOUNT_FILE, EE_PROJECT_ID

def initialize_gee():
    """
    Khởi tạo kết nối với Google Earth Engine sử dụng Service Account.
    Hàm này thay thế cho ee.Authenticate() (vốn cần trình duyệt).
    """
    try:
        # 1. Kiểm tra file key tồn tại không
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            raise FileNotFoundError(f"Không tìm thấy file Service Account tại: {SERVICE_ACCOUNT_FILE}")

        # 2. Tạo Credentials từ file JSON
        # Scope này cấp quyền truy cập đầy đủ vào Earth Engine
        scopes = ['https://www.googleapis.com/auth/earthengine']
        
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, 
            scopes=scopes
        )

        # 3. Khởi tạo GEE với Credentials và Project ID
        # Lưu ý: project=EE_PROJECT_ID rất quan trọng để tính tiền/quota đúng project
        ee.Initialize(
            credentials=credentials,
            project=EE_PROJECT_ID
        )
        
        print(f"✅ [AUTH] Đã kết nối GEE thành công với Project: {EE_PROJECT_ID}")
        return True

    except Exception as e:
        print(f"❌ [AUTH] Lỗi xác thực GEE: {e}")
        # Nếu lỗi authenticate, ta nên dừng chương trình luôn vì không làm gì được nữa
        raise e

def get_gcs_credentials():
    """
    Trả về đối tượng credentials để dùng cho thư viện Google Cloud Storage (nếu cần).
    Thường thì thư viện storage.Client() tự đọc biến môi trường, nhưng hàm này giúp explicit hơn.
    """
    return service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
