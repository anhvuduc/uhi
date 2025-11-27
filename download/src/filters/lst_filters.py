import ee
from .common import bitwise_extract

def apply_mask_lst_modis(image, data_bands, qc_band):
    """
    Áp dụng mask chất lượng (QC) cho dữ liệu MODIS LST (MOD21A1D/MYD21A1D).
    
    LƯU Ý: Hàm này KHÔNG lọc theo ngưỡng nhiệt độ (Min/Max). 
    Việc lọc giá trị ngoại lai (Outliers) nên được thực hiện ở bước Post-processing 
    (Local machine) bằng phương pháp thống kê (ví dụ: P0.5 - P99.5).

    Tài liệu tham khảo QC Bits:
    https://developers.google.com/earth-engine/datasets/catalog/MODIS_061_MYD21A1D
    
    Args:
        image (ee.Image): Ảnh đầu vào.
        data_bands (list): Danh sách các band dữ liệu cần giữ lại.
        qc_band (str): Tên band QC (thường là 'QC').

    Returns:
        ee.Image: Ảnh đã được lọc QC, scale và đổi đơn vị sang Celsius.
    """
    qc = image.select(qc_band)

    # --- 1. Mandatory QA Flags (Bits 0-1) ---
    # 0: Pixel produced, good quality -> Giữ
    # 1: Pixel produced, nominal quality -> Giữ
    # 2: Pixel not produced due to cloud -> Lọc
    # 3: Pixel not produced due to other reasons -> Lọc
    mandatory_qa = bitwise_extract(qc, 0, 1)
    mask_mandatory = mandatory_qa.lte(1)

    # --- 2. Data Quality Flag (Bits 2-3) ---
    # 0: Good data quality -> Giữ
    # 1: Missing pixel -> Lọc
    # 2: Fairly calibrated -> Giữ (Giữ lại để đảm bảo tính liên tục, lọc sau nếu cần)
    # 3: Poorly calibrated -> Lọc
    data_quality = bitwise_extract(qc, 2, 3)
    mask_data_quality = data_quality.eq(0).Or(data_quality.eq(2))

    # --- 3. LST Error Flag (Bits 14-15) ---
    # 0: Average LST error > 2K (Sai số lớn) -> Lọc
    # >= 1: Average LST error <= 2K -> Giữ
    lst_error = bitwise_extract(qc, 14, 15)
    mask_lst_error = lst_error.gte(1)

    # --- TỔNG HỢP QC MASK ---
    # Pixel phải thỏa mãn cả 3 điều kiện
    final_qc_mask = mask_mandatory.And(mask_data_quality).And(mask_lst_error)

    # --- 4. Xử lý Band & Đổi đơn vị ---
    scaled_bands = []
    for band in data_bands:
        b = image.select(band)
        
        if band == 'LST_1KM':
            # Chuyển đổi Kelvin -> Celsius
            # Áp dụng mask QC để loại bỏ mây/lỗi
            s = b.subtract(273.15).updateMask(final_qc_mask)
        
        elif band == 'View_Angle':
            # View Angle MODIS: Offset -65
            # KHÔNG áp dụng mask lên band góc nhìn (để giữ thông tin hình học nếu cần nội suy)
            s = b.subtract(65)
            
        elif band == 'View_Time':
            # Giữ nguyên View Time
            s = b
            
        else:
            # Các band khác (nếu có) giữ nguyên
            s = b
            
        scaled_bands.append(s)

    return ee.Image.cat(scaled_bands).copyProperties(image, image.propertyNames())


def apply_mask_lst_viirs(image, data_bands, qc_band):
    """
    Áp dụng mask chất lượng (QC) cho dữ liệu VIIRS LST (VNP21A1D/VNP21A1N).
    
    LƯU Ý: Hàm này KHÔNG lọc theo ngưỡng nhiệt độ (Min/Max).
    
    Tài liệu tham khảo:
    https://developers.google.com/earth-engine/datasets/catalog/NASA_VIIRS_002_VNP21A1D
    
    Args:
        image (ee.Image): Ảnh đầu vào.
        data_bands (list): Danh sách các band dữ liệu cần giữ lại.
        qc_band (str): Tên band QC.

    Returns:
        ee.Image: Ảnh đã được lọc QC, scale và đổi đơn vị sang Celsius.
    """
    qc = image.select(qc_band)

    # --- 1. Mandatory QA Flags (Bits 0-1) ---
    # 0: Good -> Giữ
    # 1: Unreliable/Nominal -> Giữ
    # 2: Cloud -> Lọc
    # 3: Other -> Lọc
    mandatory_qa = bitwise_extract(qc, 0, 1)
    mask_mandatory = mandatory_qa.lte(1)

    # --- 2. Data Quality Flag (Bits 2-3) ---
    # 0: Good -> Giữ
    # 1: Missing -> Lọc
    # 2: Fair -> Giữ
    # 3: Poor -> Lọc
    data_quality = bitwise_extract(qc, 2, 3)
    mask_data_quality = data_quality.eq(0).Or(data_quality.eq(2))

    # --- 3. LST Accuracy/Error (Bits 14-15) ---
    # 0: Error > 2K (Poor) -> Lọc
    # >= 1: Error <= 2K -> Giữ
    lst_accuracy = bitwise_extract(qc, 14, 15)
    mask_lst_accuracy = lst_accuracy.gte(1)

    # --- TỔNG HỢP QC MASK ---
    final_qc_mask = mask_mandatory.And(mask_data_quality).And(mask_lst_accuracy)

    # --- 4. Xử lý Band & Đổi đơn vị ---
    scaled_bands = []
    for band in data_bands:
        b = image.select(band)
        
        if band == 'LST_1KM':
            # Chuyển đổi Kelvin -> Celsius & Áp dụng QC Mask
            s = b.subtract(273.15).updateMask(final_qc_mask)
            
        elif band == 'View_Angle':
            # VIIRS View Angle: Offset -65 (tương tự MODIS trong collection VNP21)
            s = b.subtract(65)
            
        else:
            s = b
            
        scaled_bands.append(s)

    return ee.Image.cat(scaled_bands).copyProperties(image, image.propertyNames())

"""
================================================================================
GHI CHÚ CHI TIẾT VỀ BỘ LỌC MODIS VÀ VIIRS (LST)
================================================================================

1. NGUYÊN LÝ CHUNG:
   - Cả MODIS (MOD21/MYD21) và VIIRS (VNP21) đều cung cấp band QC (Quality Control) chứa các bit flags mô tả chất lượng pixel.
   - Mục tiêu của bộ lọc này là loại bỏ các pixel bị mây che phủ, lỗi hiệu chuẩn (calibration error), hoặc có sai số LST quá lớn (> 2K).
   - Giữ lại các pixel có chất lượng "Good" và "Nominal/Fair" để đảm bảo tính liên tục của dữ liệu không gian, chấp nhận sai số trong ngưỡng cho phép.

2. SO SÁNH CHI TIẾT BIT MASK:

   A. MODIS (MOD21/MYD21):
      - Mandatory QA (Bits 0-1):
        + 0 (Good): Giữ.
        + 1 (Nominal): Giữ.
        + 2 (Cloud), 3 (Other): Loại bỏ.
      - Data Quality (Bits 2-3):
        + 0 (Good): Giữ.
        + 2 (Fairly calibrated): Giữ.
        + 1 (Missing), 3 (Poor): Loại bỏ.
      - LST Error (Bits 14-15):
        + 0 (Error > 2K): Loại bỏ.
        + 1, 2, 3 (Error <= 2K): Giữ.

   B. VIIRS (VNP21):
      - Mandatory QA (Bits 0-1):
        + 0 (Good): Giữ.
        + 1 (Unreliable/Nominal): Giữ (Tuy tên là Unreliable nhưng trong ngữ cảnh VNP21 thường được coi là chấp nhận được nếu các flag khác tốt).
        + 2 (Cloud), 3 (Other): Loại bỏ.
      - Data Quality (Bits 2-3):
        + 0 (Good): Giữ.
        + 2 (Fair): Giữ.
        + 1 (Missing), 3 (Poor): Loại bỏ.
      - LST Accuracy (Bits 14-15):
        + 0 (Error > 2K): Loại bỏ.
        + 1, 2, 3 (Error <= 2K): Giữ.

3. XỬ LÝ GIÁ TRỊ (PROCESSING):
   - Đơn vị: Cả hai đều được chuyển từ Kelvin sang Celsius (K - 273.15).
   - View Angle: Đều áp dụng offset -65 để đưa về góc nhìn thực tế.
   - Outliers: Bộ lọc này KHÔNG cắt ngưỡng nhiệt độ cứng (Min/Max) để tránh làm mất dữ liệu cực đoan thực tế.
================================================================================
"""