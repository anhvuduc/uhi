import ee
import datetime
from src.auth import initialize_gee
from highres.config import S2_COLLECTION, S2_BANDS

def mask_s2_clouds(image):
    """
    Áp dụng bộ lọc mây nâng cao cho Sentinel-2 sử dụng kênh QA60 và dải SCL.
    """
    qa = image.select('QA60')
    cloud_bit_mask = 1 << 10
    cirrus_bit_mask = 1 << 11
    
    # 1. Mặt nạ QA60: Không có mây (bit 10 = 0) và không có cirrus (bit 11 = 0)
    qa_mask = qa.bitwiseAnd(cloud_bit_mask).eq(0).And(
              qa.bitwiseAnd(cirrus_bit_mask).eq(0))
              
    # 2. Mặt nạ SCL (Scene Classification Layer):
    # Chỉ giữ lại các pixel có nhãn:
    # 4 (Vegetation), 5 (Not Vegetated), 6 (Water), 7 (Unclassified), 11 (Snow)
    scl = image.select('SCL')
    scl_mask = scl.eq(4).Or(scl.eq(5)).Or(scl.eq(6)).Or(scl.eq(7)).Or(scl.eq(11))
    
    final_mask = qa_mask.And(scl_mask)
    
    # Chia cho 10000 để đưa về Reflectance thực tế [0, 1]
    masked = image.updateMask(final_mask).divide(10000.0)
    return masked.copyProperties(image, image.propertyNames())

def calculate_s2_indices(image):
    """
    Tính toán các chỉ số phổ NDVI, EVI và LSE (phương pháp Sobrino 2004) cho Sentinel-2.
    """
    # Kênh phổ cần dùng
    b8 = image.select('B8') # NIR
    b4 = image.select('B4') # Red
    b2 = image.select('B2') # Blue
    b11 = image.select('B11') # SWIR 1
    
    # 1. Tính NDVI
    ndvi = image.normalizedDifference(['B8', 'B4']).rename('NDVI')
    
    # 2. Tính EVI
    # Công thức: 2.5 * (B8 - B4) / (B8 + 6 * B4 - 7.5 * B2 + 1)
    evi = image.expression(
        '2.5 * ((B8 - B4) / (B8 + 6.0 * B4 - 7.5 * B2 + 1.0))',
        {'B8': b8, 'B4': b4, 'B2': b2}
    ).rename('EVI')
    
    # 3. Tính LSE (Land Surface Emissivity) theo Sobrino et al. (2004)
    # Pv (Proportion of Vegetation) = ((NDVI - 0.2) / 0.3) ^ 2, giới hạn trong khoảng [0, 1]
    pv = ndvi.subtract(0.2).divide(0.3).clamp(0, 1).pow(2)
    
    # Thỏa hiệp hiệu ứng hốc (Cavity effect) cho mixed pixels:
    # epsilon = epsilon_v * Pv + epsilon_s * (1 - Pv) + C
    # với epsilon_v = 0.99, epsilon_s = 0.97, F' = 0.55
    # C = (1 - epsilon_s) * epsilon_v * F' * (1 - Pv) = 0.016335 * (1 - Pv)
    # => epsilon = 0.99 * Pv + 0.97 * (1 - Pv) + 0.016335 * (1 - Pv) = 0.99 * Pv + 0.986335 * (1 - Pv)
    mixed_lse = pv.multiply(0.99).add(ee.Image(1).subtract(pv).multiply(0.986335))
    
    # Áp dụng các điều kiện biên:
    # Nếu NDVI < 0.2: lse = 0.97
    # Nếu NDVI > 0.5: lse = 0.99
    # Ngược lại: tính theo mixed_lse
    lse = mixed_lse.where(ndvi.lt(0.2), 0.97).where(ndvi.gt(0.5), 0.99).rename('LSE')
    
    # Trả về ảnh gồm các band chỉ số và SWIR1 (B11)
    return ee.Image.cat([ndvi, evi, lse, b11.rename('B11')]).copyProperties(image, image.propertyNames())

def get_s2_available_dates(roi, start_date, end_date):
    """
    Quét qua ImageCollection Sentinel-2 SR, tìm kiếm các ngày có ảnh sạch mây
    thỏa mãn điều kiện chất lượng và trả về danh sách các ngày duy nhất (YYYY-MM-DD).
    """
    initialize_gee()
    
    # Bounding box của ROI
    roi_bounds = roi.geometry().bounds()
    
    # Lọc bộ dữ liệu (chỉ giữ lại bộ lọc độ che phủ mây cơ bản để tránh quá khắt khe)
    col = ee.ImageCollection(S2_COLLECTION) \
            .filterBounds(roi_bounds) \
            .filterDate(start_date, end_date) \
            .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 15.0))
            
    # Lấy thông tin ngày
    dates_list = col.map(lambda img: ee.Feature(None, {
        'date': img.date().format('YYYY-MM-dd')
    })).aggregate_array('date').getInfo()
    
    # Loại bỏ trùng lặp và sắp xếp
    unique_dates = sorted(list(set(dates_list)))
    print(f"📊 [S2-SCAN] Tìm thấy {len(unique_dates)} ngày có ảnh Sentinel-2 hợp lệ từ {start_date} đến {end_date}.")
    return unique_dates

def process_s2_for_date(roi, date_str):
    """
    Tải, lọc mây, tính toán chỉ số và ghép ảnh Sentinel-2 cho một ngày cụ thể.
    """
    initialize_gee()
    roi_bounds = roi.geometry().bounds()
    
    start_dt = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    end_dt = start_dt + datetime.timedelta(days=1)
    
    start_date_str = start_dt.strftime('%Y-%m-%d')
    end_date_str = end_dt.strftime('%Y-%m-%d')
    
    # Lấy collection trong ngày và áp dụng mask mây
    col = ee.ImageCollection(S2_COLLECTION) \
            .filterBounds(roi_bounds) \
            .filterDate(start_date_str, end_date_str) \
            .map(mask_s2_clouds)
            
    # Tính toán các chỉ số
    processed_col = col.map(calculate_s2_indices)
    
    # Lấy projection gốc từ ảnh đầu tiên trong collection (band B4 10m) để thiết lập cho ảnh mosaic
    native_proj = ee.Image(col.first()).select('B4').projection()
    
    # Ghép ảnh (Mosaic) 10m và gán projection gốc
    s2_10m = processed_col.mosaic().select(S2_BANDS).setDefaultProjection(native_proj)
    
    # Gom nhóm pixel từ lưới 10m sang lưới 100m dùng reduceResolution(mean) & reproject
    s2_100m = s2_10m.reduceResolution(
        reducer=ee.Reducer.mean(),
        maxPixels=1024
    ).reproject(
        crs='EPSG:4326',
        scale=100
    ).clip(roi.geometry().bounds()).toFloat()
    
    return s2_100m
