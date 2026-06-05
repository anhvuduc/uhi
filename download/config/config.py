import os
from pathlib import Path

# ========================================================
# 1. CẤU HÌNH HỆ THỐNG & ĐƯỜNG DẪN (SYSTEM & PATHS)
# ========================================================

# Đường dẫn gốc của dự án (khi chạy trong Docker, nó sẽ là /opt/airflow hoặc /app)
BASE_DIR = Path(__file__).resolve().parent.parent

# Đường dẫn file Service Account (được mount từ máy thật vào container)
# Khuyến nghị đặt tại: raster_pipeline/config/service_account.json
SERVICE_ACCOUNT_FILE = os.getenv(
    'GOOGLE_APPLICATION_CREDENTIALS',
    os.path.join(BASE_DIR, 'config', 'service_account.json')
)

# Project ID của Google Cloud (lấy từ tên project GEE của bạn)
EE_PROJECT_ID = 'ee-docxducanh'

# Thư mục lưu dữ liệu tải về (Mapped Volume từ ổ D:/Data_RemoteSensing)
LOCAL_DATA_DIR = os.getenv('DATA_DIR', '/opt/data')

# ========================================================
# 2. KHU VỰC NGHIÊN CỨU (REGIONS OF INTEREST - ROIs)
# ========================================================
# Mapping: Tên thành phố (viết thường) -> Asset ID trên GEE
ROIS = {
    'hanoi': 'users/anhvuduc/hanoi',
    'haiphong': 'users/anhvuduc/haiphong_blv',
    'danang': 'users/anhvuduc/dn_hs',
    'hcm': 'users/anhvuduc/hcm'
    # 'binhduong': 'users/anhvuduc/binhduong' # (Optional: Uncomment nếu cần)
}

# ========================================================
# 3. CẤU HÌNH VỆ TINH (COLLECTIONS & BANDS)
# ========================================================
# Cấu trúc này giúp code xử lý tự động biết cần lấy band nào và band QC nào.

SATELLITE_CONFIG = {
    # --- NHÓM NHIỆT ĐỘ BỀ MẶT (LST) ---
    
    # MODIS Terra/Aqua LST (1km)
    'MYD21A1D': {
        'id': 'MODIS/061/MYD21A1D',
        'type': 'LST',
        'data_bands': ['LST_1KM', 'View_Time', 'View_Angle', 'Emis_29', 'Emis_31', 'Emis_32'],
        'qc_band': 'QC',
        'scale': 1000
    },
    'MYD21A1N': {
        'id': 'MODIS/061/MYD21A1N',
        'type': 'LST',
        'data_bands': ['LST_1KM', 'View_Time', 'View_Angle', 'Emis_29', 'Emis_31', 'Emis_32'],
        'qc_band': 'QC',
        'scale': 1000
    },
    'MOD21A1D': {
        'id': 'MODIS/061/MOD21A1D',
        'type': 'LST',
        'data_bands': ['LST_1KM', 'View_Time', 'View_Angle', 'Emis_29', 'Emis_31', 'Emis_32'    ],
        'qc_band': 'QC',
        'scale': 1000
    },
    'MOD21A1N': {
        'id': 'MODIS/061/MOD21A1N',
        'type': 'LST',
        'data_bands': ['LST_1KM', 'View_Time', 'View_Angle', 'Emis_29', 'Emis_31', 'Emis_32'],
        'qc_band': 'QC',
        'scale': 1000
    },

    # VIIRS SNPP LST (1km)
    'VNP21A1D': {
        'id': 'NASA/VIIRS/002/VNP21A1D',
        'type': 'LST',
        'data_bands': ['LST_1KM', 'View_Time', 'View_Angle', 'Emis_14', 'Emis_15', 'Emis_16'],
        'qc_band': 'QC', 
        'scale': 1000
    },
    'VNP21A1N': {
        'id': 'NASA/VIIRS/002/VNP21A1N',
        'type': 'LST',
        'data_bands': ['LST_1KM', 'View_Time', 'View_Angle', 'Emis_14', 'Emis_15', 'Emis_16'],
        'qc_band': 'QC',
        'scale': 1000
    },

    # --- NHÓM CHỈ SỐ THỰC VẬT (VEGETATION INDICES) ---

    # MODIS Vegetation Indices (Combined Terra/Aqua 16-day)
    'MXD13A1': {
        'id': ['MODIS/061/MOD13A1', 'MODIS/061/MYD13A1'], # List ID để merge
        'type': 'VI',
        'data_bands': ['NDVI', 'EVI', 'ViewZenith', 'SolarZenith', 'RelativeAzimuth'],
        'qc_band': 'DetailedQA',
        'scale': 1000,
        'product_name': 'MXD13A1', # Tên định danh cho file output
        'date_tolerance_days': 9, # VI 16-day product (Allow ~8-9 days gap)
        'force_historical_full': True # Force full month for past months
    },

    # VIIRS Vegetation Indices (16-day, 500m)
    'VNP13A1': {
        'id': 'NASA/VIIRS/002/VNP13A1',
        'type': 'VI',
        'data_bands': [
            'NDVI', 'EVI', 'EVI2', 
            'view_zenith_angle', 'sun_zenith_angle', 'relative_azimuth_angle', 'pixel_reliability'
        ],
        'qc_band': 'VI_Quality', # Lưu ý: VIIRS dùng VI_Quality
        'scale': 1000, #
        'date_tolerance_days': 9, # VI 16-day product
        'force_historical_full': True # Force full month for past months
    }
}

# ========================================================
# 4. THÔNG SỐ EXPORT MẶC ĐỊNH
# ========================================================
EXPORT_DEFAULTS = {
    'crs': 'EPSG:4326',       # Hệ tọa độ WGS84
    'file_format': 'GeoTIFF', # Định dạng file
    'max_pixels': 1e13        # Giới hạn pixel của GEE
}

# ========================================================
# 5. CẤU HÌNH CLOUD STORAGE (GCS)
# ========================================================
GCS_CONFIG = {
    # Tên Bucket bạn vừa tạo trên Google Cloud Console
    # Lưu ý: Chỉ điền tên, không có "gs://" ở đầu
    'bucket_name': 'uhi-vn', 
    
    # Thư mục gốc trong bucket để chứa dữ liệu dự án này
    # Giúp bucket gọn gàng nếu bạn dùng nó cho nhiều việc khác nhau
    'base_folder': 'raw', 
    
    # Cấu hình tải về (Download settings)
    'download_threads': 64,       # Số luồng tải song song (tùy mạng, 4-16)
    'auto_delete_cloud': False,  # True: Tải xong xóa trên Cloud (Tiết kiệm), False: Giữ làm Backup
}