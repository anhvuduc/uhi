import ee
import datetime
from src.auth import initialize_gee
from highres.config import LST_COLLECTIONS

def bitwise_extract(value, from_bit, to_bit):
    """
    Trích xuất các bit cụ thể từ một dải số nguyên 16-bit.
    """
    mask_size = to_bit - from_bit + 1
    mask = (1 << mask_size) - 1
    return value.rightShift(from_bit).bitwiseAnd(mask)

def process_single_lst_image(image, lst_band_name, qc_band_name, other_band_names, scale_factor=0.02):
    """
    Lọc chất lượng QC, quy đổi nhiệt độ sang độ C, tạo ma trận trọng số (LST_Weight),
    trích xuất mức sai số LST gốc (LST_Error) và giữ lại các kênh thông tin khác.
    """
    qc = image.select(qc_band_name)
    lst = image.select(lst_band_name)
    
    # 1. Mandatory QA Flag (Bits 0-1):
    # 0: Good, 1: Nominal -> Giữ. Các giá trị khác -> Loại bỏ.
    mandatory_qa = bitwise_extract(qc, 0, 1)
    mask_mandatory = mandatory_qa.lte(1)
    
    # 2. Data Quality Flag (Bits 2-3):
    # 0: Good, 2: Fair -> Giữ. Các giá trị khác -> Loại bỏ.
    data_quality = bitwise_extract(qc, 2, 3)
    mask_data_quality = data_quality.eq(0).Or(data_quality.eq(2))
    # 3. LST Error Flag (Bits 14-15):
    # Loại bỏ khi sai số > 2K (giá trị = 0). Giữ lại các mức 1, 2, 3.
    lst_error = bitwise_extract(qc, 14, 15)
    mask_lst_error = lst_error.gte(1)
    
    # Kết hợp các điều kiện lọc chất lượng
    # (Đã loại bỏ lọc Bits 4-5 vì quá nghiêm ngặt; Mandatory QA và LST Error đã đủ để loại bỏ mây và sai số cao)
    final_qc_mask = mask_mandatory.And(mask_data_quality).And(mask_lst_error)
    
    # Quy đổi nhiệt độ Kelvin -> Celsius (Sử dụng scale factor được truyền vào)
    lst_celsius = lst.updateMask(final_qc_mask).multiply(scale_factor).subtract(273.15).rename('LST_Celsius')
    
    # Kênh sai số LST gốc (Bits 14-15: 0=Poor/Acc>=2K, 1=Marginal/Acc 1.5-2K, 2=Good/Acc 1-1.5K, 3=Excellent/Acc<1K)
    # Ta cũng updateMask để chỉ giữ lại các pixel có chất lượng hợp lệ tổng thể
    lst_error_band = lst_error.updateMask(final_qc_mask).rename('LST_Error')
    
    # Tính toán ma trận trọng số huấn luyện (Loss Weighting) dựa trên Bit 14-15:
    # Bit 14-15 = 3 (Sai số < 1K) -> Trọng số W = 1.0
    # Bit 14-15 = 2 (Sai số 1 - 1.5K) -> Trọng số W = 0.8
    # Bit 14-15 = 1 (Sai số 1.5 - 2K) -> Trọng số W = 0.5
    # Bit 14-15 = 0 (Sai số > 2K) -> Trọng số W = 0 (Bị mask)
    weight = lst_error.expression(
        '(b(0) == 3) ? 1.0 : ((b(0) == 2) ? 0.8 : ((b(0) == 1) ? 0.5 : 0.0))'
    ).updateMask(final_qc_mask).rename('LST_Weight')
    
    # Thu thập tất cả các kênh khác cần giữ lại và áp dụng mặt nạ chất lượng
    bands_to_cat = [lst_celsius]
    for bname in other_band_names:
        band_img = image.select(bname)
        if bname == 'View_Angle':
            # Đồng bộ xử lý góc nhìn View_Angle với lst_filters.py (áp dụng offset -65)
            band_img = band_img.subtract(65)
        bands_to_cat.append(band_img.updateMask(final_qc_mask))
        
    bands_to_cat.extend([lst_error_band, weight])
    
    return ee.Image(ee.Image.cat(bands_to_cat).copyProperties(image, image.propertyNames()))

def process_lst_for_date_window(roi, date_str, collection_key):
    """
    Tính trung bình cộng (mean composite) các giá trị nhiệt độ và các kênh thông tin
    trong khoảng 5 ngày: [Landsat_date - 2 ngày, Landsat_date + 2 ngày] xung quanh ngày bay của Landsat.
    """
    initialize_gee()
    roi_bounds = roi.geometry().bounds()
    
    # Xác định khoảng thời gian 3 ngày xung quanh ngày mốc Landsat (hôm trước, hôm nay, hôm sau)
    target_dt = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    start_dt = target_dt - datetime.timedelta(days=1)
    end_dt = target_dt + datetime.timedelta(days=2) # Exclusive (đến hết ngày +1)
    
    start_date_str = start_dt.strftime('%Y-%m-%d')
    end_date_str = end_dt.strftime('%Y-%m-%d')
    
    cfg = LST_COLLECTIONS[collection_key]
    col_id = cfg['id']
    
    # Xác định các dải kênh của từng sản phẩm LST và scale factor tương ứng
    if collection_key in ['MOD21A1D', 'MYD21A1D']:
        lst_band = 'LST_1KM'
        qc_band = 'QC'
        other_bands = ['QC', 'View_Angle', 'View_Time', 'Emis_29', 'Emis_31', 'Emis_32']
        out_bands = ['LST_Celsius', 'QC', 'View_Angle', 'View_Time', 'Emis_29', 'Emis_31', 'Emis_32', 'LST_Error', 'LST_Weight']
        scale_factor = 1.0
    elif collection_key == 'VNP21A1D':
        lst_band = 'LST_1KM'
        qc_band = 'QC'
        other_bands = ['QC', 'View_Angle', 'View_Time', 'Emis_14', 'Emis_15', 'Emis_16']
        out_bands = ['LST_Celsius', 'QC', 'View_Angle', 'View_Time', 'Emis_14', 'Emis_15', 'Emis_16', 'LST_Error', 'LST_Weight']
        scale_factor = 0.02
    else:
        raise ValueError(f"Không hỗ trợ bộ sưu tập LST: {collection_key}")
        
    # Lọc collection
    col = ee.ImageCollection(col_id) \
            .filterBounds(roi_bounds) \
            .filterDate(start_date_str, end_date_str)
            
    count = col.size().getInfo()
    if count == 0:
        return None
        
    # Xử lý chất lượng và nội suy bilinear cho từng ảnh trước khi merge (Mean) để bảo toàn projection gốc
    processed_col = col.map(lambda img: process_single_lst_image(img, lst_band, qc_band, other_bands, scale_factor).resample('bilinear'))
    
    # Tính trung bình cộng (Mean)
    mean_img = processed_col.mean()
    
    # Reproject đưa LST lên lưới 100m đồng bộ và clip theo ROI bounds
    final_image = mean_img.reproject(
        crs='EPSG:4326',
        scale=100
    ).clip(roi_bounds).select(out_bands).toFloat()
    
    return final_image
