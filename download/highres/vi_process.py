import ee
import datetime
from src.auth import initialize_gee
from src.filters.vi_filters import apply_mask_vi_modis, apply_mask_vi_viirs

def process_vi_for_date_window(roi, date_str, collection_key):
    """
    Tính trung bình cộng (Mean composite) các chỉ số thực vật trong khoảng 16 ngày
    [Landsat_date - 8 ngày, Landsat_date + 8 ngày] xung quanh ngày bay của Landsat.
    Quy đổi về độ phân giải 100m sử dụng nội suy Bilinear.
    """
    initialize_gee()
    roi_bounds = roi.geometry().bounds()
    
    target_dt = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    start_dt = target_dt - datetime.timedelta(days=8)
    end_dt = target_dt + datetime.timedelta(days=9) # Exclusive
    
    start_date_str = start_dt.strftime('%Y-%m-%d')
    end_date_str = end_dt.strftime('%Y-%m-%d')
    
    # 1. Định cấu hình dữ liệu và dải phổ tương ứng
    if collection_key == 'MXD13A1':
        # Gộp hai bộ sưu tập MOD13A1 (Terra) và MYD13A1 (Aqua) 16-day 500m
        col_mod = ee.ImageCollection('MODIS/061/MOD13A1')
        col_myd = ee.ImageCollection('MODIS/061/MYD13A1')
        col = col_mod.merge(col_myd) \
                     .filterBounds(roi_bounds) \
                     .filterDate(start_date_str, end_date_str)
                     
        data_bands = ['NDVI', 'EVI', 'ViewZenith', 'SolarZenith', 'RelativeAzimuth', 
                      'sur_refl_b01', 'sur_refl_b02', 'sur_refl_b03', 'sur_refl_b07']
        qc_band = 'DetailedQA'
        
        count = col.size().getInfo()
        if count == 0:
            return None
            
        # Áp dụng bộ lọc chất lượng của MODIS và resample bilinear
        processed_col = col.map(lambda img: apply_mask_vi_modis(img, data_bands, qc_band).resample('bilinear'))
        
    elif collection_key == 'VNP13A1':
        # VIIRS Vegetation Indices 16-day 500m
        col = ee.ImageCollection('NASA/VIIRS/002/VNP13A1') \
                .filterBounds(roi_bounds) \
                .filterDate(start_date_str, end_date_str)
                
        data_bands = [
            'NDVI', 'EVI', 'EVI2', 
            'view_zenith_angle', 'sun_zenith_angle', 'relative_azimuth_angle', 'pixel_reliability',
            'NIR_reflectance', 'SWIR1_reflectance', 'SWIR2_reflectance', 'SWIR3_reflectance', 
            'red_reflectance', 'green_reflectance', 'blue_reflectance'
        ]
        qc_band = 'VI_Quality'
        
        count = col.size().getInfo()
        if count == 0:
            return None
            
        # Áp dụng bộ lọc chất lượng của VIIRS và resample bilinear
        processed_col = col.map(lambda img: apply_mask_vi_viirs(img, data_bands, qc_band).resample('bilinear'))
        
    else:
        raise ValueError(f"Không hỗ trợ bộ dữ liệu VI: {collection_key}")
        
    # 2. Tính trung bình và nâng độ phân giải lên 100m sử dụng nội suy Bilinear
    mean_img = processed_col.mean()
    
    final_image = mean_img.reproject(
        crs='EPSG:4326',
        scale=100
    ).clip(roi_bounds).toFloat()
    
    return final_image
