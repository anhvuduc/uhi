import ee
from .common import bitwise_extract

def _scale_and_mask_bands(image, final_mask, data_bands):
    """
    Hàm nội bộ: Áp dụng mask và scale giá trị cho các band VI và Angle.
    """
    scaled_bands = []
    for band in data_bands:
        b = image.select(band)
        
        # 1. Nhóm chỉ số thực vật (NDVI, EVI, EVI2)
        # Giá trị gốc thường là Int16 (-2000 đến 10000), scale 0.0001 -> (-0.2 đến 1.0)
        if band in ['NDVI', 'EVI', 'EVI2']:
            s = b.multiply(0.0001).updateMask(final_mask)
            
        # 2. Nhóm góc quan sát (Angles)
        # MODIS/VIIRS Angle thường có scale factor là 0.01
        elif band in ['ViewZenith', 'SolarZenith', 'RelativeAzimuth', 
                      'view_zenith_angle', 'sun_zenith_angle', 'relative_azimuth_angle']:
            s = b.multiply(0.01) # Không cần mask góc để giữ hình học
            
        # 3. Các band khác (giữ nguyên hoặc xử lý riêng)
        else:
            s = b
            
        scaled_bands.append(s.rename(band))

    return ee.Image.cat(scaled_bands).copyProperties(image, image.propertyNames())


def apply_mask_vi_modis(image, data_bands, qc_band):
    """
    Bộ lọc cho MODIS Vegetation Indices (MOD13A1 / MYD13A1).
    
    Tham khảo: myd13-251006.ipynb
    Band QC: 'DetailedQA' (hoặc 'VI_Quality' tùy collection version)
    """
    qc = image.select(qc_band)

    # --- 1. VI Quality (Bits 0-1) ---
    # 0 (00): VI produced with good quality -> Giữ
    # 1 (01): VI produced, but check other QA -> Giữ
    # 2 (10): Pixel produced, but most probably cloudy -> Lọc
    # 3 (11): Pixel not produced due to other reasons -> Lọc
    vi_qa = bitwise_extract(qc, 0, 1)
    mask_quality = vi_qa.lte(1)

    # --- 2. VI Usefulness (Bits 2-5) ---
    # Scale từ 0 (Chất lượng cao nhất) đến 15 (Không dùng được)
    # Trong script của bạn dùng ngưỡng 8.
    # <= 8 bao gồm: Excellent, Good, Acceptable, Marginal, Pass, Questionable, Poor, Cloud Shadow, Snow/Ice
    # Khuyến nghị: Nếu muốn sạch hơn thì dùng lte(4) hoặc lte(2).
    vi_usefulness = bitwise_extract(qc, 2, 5)
    mask_usefulness = vi_usefulness.lte(8)

    # --- TỔNG HỢP MASK ---
    final_mask = mask_quality.And(mask_usefulness)

    # Scale và trả về ảnh
    return _scale_and_mask_bands(image, final_mask, data_bands)


def apply_mask_vi_viirs(image, data_bands, qc_band):
    """
    Bộ lọc cho VIIRS Vegetation Indices (VNP13A1).
    
    Tham khảo: vnp13a1-250703.ipynb
    Band QC: 'VI_Quality'
    """
    qc = image.select(qc_band)

    # --- 1. MODLAND QA Bits (Bits 0-1) ---
    # Tương tự MODIS: 0=Good, 1=Marginal
    modland_qa = bitwise_extract(qc, 0, 1)
    mask_quality = modland_qa.lte(1)

    # --- 2. VI Usefulness (Bits 2-5) ---
    # Logic tương tự MODIS.
    # Script gốc của bạn: vi_usefulness.lte(8)
    vi_usefulness = bitwise_extract(qc, 2, 5)
    mask_usefulness = vi_usefulness.lte(8)
    
    # --- TỔNG HỢP MASK ---
    final_mask = mask_quality.And(mask_usefulness)

    # Scale và trả về ảnh
    return _scale_and_mask_bands(image, final_mask, data_bands)

# Hàm tổng quát (Alias) để gọi từ processor chung nếu cần
def apply_mask_ndvi(image, data_bands, qc_band):
    """
    Hàm Wrapper tự động phát hiện logic dựa trên tên band QC hoặc cấu trúc ảnh.
    Mặc định sử dụng logic của VIIRS (vì cấu trúc bitmask VNP13 và MOD13 tương đồng).
    """
    # Logic bitmask của MOD13 và VNP13 cơ bản giống nhau ở các bit đầu
    # Nên có thể dùng chung hàm apply_mask_viirs_vi
    return apply_mask_viirs_vi(image, data_bands, qc_band)