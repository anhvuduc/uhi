import os
from config.config import ROIS, GCS_CONFIG, SERVICE_ACCOUNT_FILE, EE_PROJECT_ID, EXPORT_DEFAULTS

# Hạn ngạch GEE (số lượng task active tối đa trong queue)
GEE_MAX_ACTIVE_TASKS = 2800

# Thư mục cơ sở lưu dữ liệu local
LOCAL_DATA_DIR = os.getenv('DATA_DIR', '/opt/airflow/data')
HIGHRES_LOCAL_DIR = os.path.join(LOCAL_DATA_DIR, 'highres')

# GCS Folder riêng cho highres
HIGHRES_GCS_FOLDER = 'highres'

# Cấu hình Landsat 8/9 LST & Indices (6 Kênh chất lượng cao nhất)
LANDSAT_COLLECTIONS = {
    'LC08': 'LANDSAT/LC08/C02/T1_L2',
    'LC09': 'LANDSAT/LC09/C02/T1_L2'
}
LANDSAT_BANDS = ['LST_Celsius', 'NDVI', 'EVI', 'NDBI', 'Emissivity', 'Band_1']

# Cấu hình MODIS & VIIRS Daily LST
LST_COLLECTIONS = {
    'MOD21A1D': {
        'id': 'MODIS/061/MOD21A1D',
        'type': 'Terra_Morning'
    },
    'MYD21A1D': {
        'id': 'MODIS/061/MYD21A1D',
        'type': 'Aqua_Noon'
    },
    'VNP21A1D': {
        'id': 'NASA/VIIRS/002/VNP21A1D',
        'type': 'VIIRS_Noon'
    }
}

MODIS_LST_BANDS = ['LST_Celsius', 'QC', 'View_Angle', 'View_Time', 'Emis_29', 'Emis_31', 'Emis_32', 'LST_Error', 'LST_Weight']
VIIRS_LST_BANDS = ['LST_Celsius', 'QC', 'View_Angle', 'View_Time', 'Emis_14', 'Emis_15', 'Emis_16', 'LST_Error', 'LST_Weight']

# Cấu hình MODIS & VIIRS 16-day VIs
VI_COLLECTIONS = {
    'MXD13A1': {
        'bands': ['NDVI', 'EVI', 'ViewZenith', 'SolarZenith', 'RelativeAzimuth', 'sur_refl_b01', 'sur_refl_b02', 'sur_refl_b03', 'sur_refl_b07']
    },
    'VNP13A1': {
        'bands': ['NDVI', 'EVI', 'EVI2', 'view_zenith_angle', 'sun_zenith_angle', 'relative_azimuth_angle', 'pixel_reliability',
                  'NIR_reflectance', 'SWIR1_reflectance', 'SWIR2_reflectance', 'SWIR3_reflectance',
                  'red_reflectance', 'green_reflectance', 'blue_reflectance']
    }
}
