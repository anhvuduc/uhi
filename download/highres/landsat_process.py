import ee
import datetime
from src.auth import initialize_gee
from highres.config import LANDSAT_COLLECTIONS

def mask_landsat_sr(image):
    """
    Áp dụng mặt nạ loại bỏ mây, bóng mây, cirrus và dilated cloud từ dải QA_PIXEL.
    """
    qa = image.select('QA_PIXEL')
    
    # Định nghĩa các bit tương ứng của QA_PIXEL
    dilated_cloud_bit = 1 << 1
    cirrus_bit = 1 << 2
    cloud_bit = 1 << 3
    cloud_shadow_bit = 1 << 4
    
    mask = qa.bitwiseAnd(dilated_cloud_bit).eq(0).And(
           qa.bitwiseAnd(cirrus_bit).eq(0)).And(
           qa.bitwiseAnd(cloud_bit).eq(0)).And(
           qa.bitwiseAnd(cloud_shadow_bit).eq(0))
           
    return image.updateMask(mask)

def update_mask_radsat_aerosol(image):
    """
    Áp dụng bộ lọc bão hòa QA_RADSAT và chất lượng khí dung SR_QA_AEROSOL.
    """
    # 1. Mặt nạ mây cơ bản từ QA_PIXEL
    image_masked = mask_landsat_sr(image)
    
    radsat = image.select('QA_RADSAT')
    aerosol = image.select('SR_QA_AEROSOL')
    
    # ---- XỬ LÝ QA_RADSAT ----
    # Bão hòa: Band 5 (bit 4), Band 6 (bit 5), Band 7 (bit 6), Band 9 (bit 8)
    # Và che khuất địa hình (bit 11). Các bit này phải bằng 0.
    sat_bits = (1 << 4) | (1 << 5) | (1 << 6) | (1 << 8) | (1 << 11)
    radsat_mask = radsat.bitwiseAnd(sat_bits).eq(0)
    
    # ---- XỬ LÝ SR_QA_AEROSOL ----
    fill_aerosol = 1 << 0
    base_aerosol_mask = aerosol.bitwiseAnd(fill_aerosol).eq(0) # Không chứa pixel trống (no fill)
    
    # Mức độ khí dung nằm ở bit 6-7
    aerosol_level = aerosol.rightShift(6).bitwiseAnd(3)
    # Thỏa hiệp: Loại bỏ khi mức độ aerosol là CAO (giá trị = 3). Giữ lại Thấp (1) và Vừa (2).
    aerosol_confidence_mask = aerosol_level.lt(3)
    
    final_aerosol_mask = base_aerosol_mask.And(aerosol_confidence_mask)
    
    return image_masked.updateMask(radsat_mask).updateMask(final_aerosol_mask)

def calculate_aerosol_ratio(image, roi):
    """
    Tính tỉ lệ pixel nhiễm khí dung mức Vừa (Medium Aerosol) trong vùng ROI.
    """
    aerosol = image.select('SR_QA_AEROSOL')
    aerosol_level = aerosol.rightShift(6).bitwiseAnd(3)
    medium_aerosol = aerosol_level.eq(2)
    
    # Chỉ xét trên những pixel hợp lệ sau khi đã lọc mây/RADSAT/Aerosol
    valid_mask = image.select('ST_B10').mask()
    
    stats = ee.Image.cat([
        medium_aerosol.rename('medium').updateMask(valid_mask),
        ee.Image(1).rename('total').updateMask(valid_mask)
    ]).reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=roi.geometry(),
        scale=100,
        maxPixels=1e9
    )
    
    try:
        info = stats.getInfo()
        medium_sum = info.get('medium', 0)
        total_sum = info.get('total', 0)
        if total_sum and total_sum > 0:
            return float(medium_sum) / float(total_sum)
        else:
            return 0.0
    except Exception as e:
        print(f"  ⚠️ [AEROSOL-LOG] Lỗi khi tính toán tỉ lệ khí dung: {e}")
        return 0.0

def get_landsat_available_dates(roi, start_date, end_date):
    """
    Tìm kiếm các ngày có ảnh Landsat 8 và 9 sạch mây (CLOUD_COVER < 15) cho vùng ROI.
    """
    initialize_gee()
    roi_bounds = roi.geometry().bounds()
    
    col_l8 = ee.ImageCollection(LANDSAT_COLLECTIONS['LC08']) \
               .filterBounds(roi_bounds) \
               .filterDate(start_date, end_date) \
               .filter(ee.Filter.lt('CLOUD_COVER', 15.0))
               
    col_l9 = ee.ImageCollection(LANDSAT_COLLECTIONS['LC09']) \
               .filterBounds(roi_bounds) \
               .filterDate(start_date, end_date) \
               .filter(ee.Filter.lt('CLOUD_COVER', 15.0))
               
    col = col_l8.merge(col_l9)
    
    dates_list = col.map(lambda img: ee.Feature(None, {
        'date': img.date().format('YYYY-MM-dd')
    })).aggregate_array('date').getInfo()
    
    unique_dates = sorted(list(set(dates_list)))
    print(f"📊 [LANDSAT-SCAN] Tìm thấy {len(unique_dates)} ngày có ảnh Landsat 8/9 sạch mây từ {start_date} đến {end_date}.")
    return unique_dates

def process_landsat_for_date(roi, date_str):
    """
    Tải ảnh Landsat 8/9 trùng ngày, áp dụng bộ lọc mây nâng cao, quy đổi nhiệt độ ST_B10 độ C,
    tính toán các chỉ số bề mặt chất lượng cao (NDVI, EVI, NDBI, Emissivity, Coastal Aerosol/Band 1)
    ở lưới 30m, sau đó coarsen lên lưới 100m dùng reduceResolution(mean).
    """
    initialize_gee()
    roi_bounds = roi.geometry().bounds()
    
    start_dt = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    end_dt = start_dt + datetime.timedelta(days=1)
    
    start_date_str = start_dt.strftime('%Y-%m-%d')
    end_date_str = end_dt.strftime('%Y-%m-%d')
    
    # 1. Lọc ảnh trùng ngày
    col_l8 = ee.ImageCollection(LANDSAT_COLLECTIONS['LC08']) \
               .filterBounds(roi_bounds) \
               .filterDate(start_date_str, end_date_str)
               
    col_l9 = ee.ImageCollection(LANDSAT_COLLECTIONS['LC09']) \
               .filterBounds(roi_bounds) \
               .filterDate(start_date_str, end_date_str)
               
    col = col_l8.merge(col_l9)
    
    count = col.size().getInfo()
    if count == 0:
        return None, None
        
    # Chọn ảnh sạch nhất
    landsat_image = col.sort('CLOUD_COVER', True).first()
    
    meta_info = {
        'id': landsat_image.get('system:index').getInfo(),
        'cloud_cover': landsat_image.get('CLOUD_COVER').getInfo(),
        'collection': 'LC09' if 'LC09' in landsat_image.get('system:id').getInfo() else 'LC08'
    }
    
    # 2. Áp dụng QA masks
    processed_image = update_mask_radsat_aerosol(landsat_image)
    
    # Tính tỉ lệ aerosol
    aerosol_ratio = calculate_aerosol_ratio(processed_image, roi)
    meta_info['medium_aerosol_pixel_ratio'] = aerosol_ratio
    
    # 3. Quy đổi các băng phổ quang học (Bản Collection 2 SR scale = 0.0000275, offset = -0.2)
    b1 = processed_image.select('SR_B1').multiply(0.0000275).add(-0.2) # Coastal Aerosol (Atmospheric Opacity)
    b2 = processed_image.select('SR_B2').multiply(0.0000275).add(-0.2) # Blue
    b4 = processed_image.select('SR_B4').multiply(0.0000275).add(-0.2) # Red
    b5 = processed_image.select('SR_B5').multiply(0.0000275).add(-0.2) # NIR
    b6 = processed_image.select('SR_B6').multiply(0.0000275).add(-0.2) # SWIR 1
    
    # Kênh 1: LST_Celsius (Quy đổi nhiệt độ)
    lst_celsius = processed_image.select('ST_B10').multiply(0.00341802).add(149.0).subtract(273.15).rename('LST_Celsius')
    
    # Kênh 2: NDVI
    ndvi = b5.subtract(b4).divide(b5.add(b4)).rename('NDVI')
    
    # Kênh 3: EVI
    evi = b5.subtract(b4).multiply(2.5).divide(
        b5.add(b4.multiply(6.0)).subtract(b2.multiply(7.5)).add(1.0)
    ).rename('EVI')
    
    # Kênh 4: NDBI
    ndbi = b6.subtract(b5).divide(b6.add(b5)).rename('NDBI')
    
    # Kênh 5: Emissivity (LSE theo Sobrino 2004 với hiệu ứng hốc, kết hợp gán nhãn nước cho pixel NDVI < 0)
    pv = ndvi.subtract(0.2).divide(0.3).clamp(0, 1).pow(2)
    mixed_lse = pv.multiply(0.99).add(ee.Image(1).subtract(pv).multiply(0.986335))
    lse = mixed_lse \
        .where(ndvi.lt(0.2), 0.97) \
        .where(ndvi.lt(0.0), 0.991) \
        .where(ndvi.gt(0.5), 0.99) \
        .rename('Emissivity')
    
    # Kênh 6: Band 1 (Coastal Aerosol)
    band1 = b1.rename('Band_1')
    
    # 4. Gộp 6 kênh ở độ phân giải 30m
    landsat_30m = ee.Image.cat([lst_celsius, ndvi, evi, ndbi, lse, band1])
    
    # Lấy projection gốc từ ST_B10 để gán cho ảnh gộp
    native_proj = landsat_image.select('ST_B10').projection()
    landsat_30m = landsat_30m.setDefaultProjection(native_proj)
    
    # 5. Gom nhóm pixel lên lưới 100m sử dụng reduceResolution(mean) & reproject
    landsat_100m = landsat_30m.reduceResolution(
        reducer=ee.Reducer.mean(),
        maxPixels=1024
    ).reproject(
        crs='EPSG:4326',
        scale=100
    ).clip(roi_bounds).toFloat()
    
    return landsat_100m, meta_info
