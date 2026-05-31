Cấu hình một pipeline mới - tạo 1 folder mới tên highres bên trong folder download, xây dựng 1 dag hoàn chỉnh để tải các bộ sưu tập mới - tạo mới mọi script cần thiết trong folder này để thực hiện công việc download dữ liệu như mô tả dưới đây. Check kỹ các yêu cầu và cung cấp 1 plan hoàn chỉnh trước khi thực thie
BẢN TẢI CHI TIẾT 3 BỘ DỮ LIỆU CỐT LÕI
1. Bộ dữ liệu dẫn đường không gian: Sentinel-2 Level-2A

Bước 1 - kiểm tra ngày có dữ liệu sentinel 2 để tải các dữ liệu khác tương ứng
function maskS2clouds(image) {
  var qa = image.select('QA60');

  // Bits 10 and 11 are clouds and cirrus, respectively.
  var cloudBitMask = 1 << 10;
  var cirrusBitMask = 1 << 11;

  // Both flags should be set to zero, indicating clear conditions.
  var mask = qa.bitwiseAnd(cloudBitMask).eq(0)
      .and(qa.bitwiseAnd(cirrusBitMask).eq(0));

  // Preserve original metadata properties before dividing
  var masked = image.updateMask(mask).divide(10000);
  return masked.copyProperties(image, image.propertyNames());
}

// 1. Load your Hanoi shapefile asset
var hanoi = ee.FeatureCollection('users/anhvuduc/hcm');

// 2. GET THE RECTANGLE BOUNDING BOX
// .geometry().bounds() calculates the minimum bounding rectangle of the shapefile
var hanoiBoundingRectangle = hanoi.geometry().bounds();

// 3. Load the raw collection and apply filters
var rawCollection = ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
                      .filterBounds(hanoiBoundingRectangle) // Filter using the rectangle
                      .filterDate('2017-03-18', '2022-06-30')
                      .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 10));

// 4. Extract and format the acquisition dates from the RAW collection
var imageDates = rawCollection.map(function(image) {
  var dateStr = image.date().format('YYYY-MM-dd HH:mm:ss');
  return ee.Feature(null, {
    'date': dateStr,
    'id': image.id()
  });
}).aggregate_array('date');

print('Total number of images:', rawCollection.size());
print('Available Image Dates (UTC):', imageDates);

Ví dụ với đầu ra như thế này: 
Total number of images:
241

Available Image Dates (UTC):
List (241 elements)
0:
2017-12-22 03:34:37
1:
2018-01-03 03:24:55
2:
2018-02-12 03:24:58
3:
2018-02-12 03:24:55
4:
2018-12-02 03:34:41
5:
2019-01-06 03:35:10
6:
2019-01-06 03:34:55
7:
2019-01-13 03:25:15
8:
2019-01-13 03:25:01
9:
2019-01-13 03:25:13
10:
2019-01-13 03:24:58
11:
2019-01-28 03:24:59
12:
2019-01-28 03:25:10
13:
2019-01-28 03:24:55
14:
2019-02-07 03:24:59
15:
2019-02-07 03:25:10
16:
2019-02-07 03:24:56
17:
2019-02-10 03:35:08
18:
2019-02-10 03:34:54
19:
2019-02-10 03:34:46
20:
2019-02-12 03:25:02
21:
2019-02-15 03:35:11
22:
2019-02-15 03:34:57
23:
2019-02-15 03:34:49
24:
2019-02-17 03:25:10
25:
2019-02-17 03:24:55
26:
2019-02-20 03:34:46
27:
2019-02-25 03:34:56
28:
2019-02-25 03:34:48
29:
2019-02-27 03:25:11
30:
2019-02-27 03:24:57
31:
2019-02-27 03:25:09
32:
2019-02-27 03:24:54
33:
2019-03-02 03:34:52
34:
2019-03-02 03:34:44
35:
2019-03-17 03:35:10
36:
2019-03-17 03:34:48


→ MOSAIC CÁC SENCE CÙNG NGÀY THÀNH 1 ẢNH DUY NHẤT - đảm bảo projection của các bộ dữ liệu là giống nhau (kỳ vọng là epsg:4326)
SAU KHI XÁC ĐỊNH ĐƯỢC CÁC NGÀY CẦN TẢI - THỰC HIỆN TẢI ĐỒNG THỜI CHO CÁC BỘ DỮ LIỆU MODIS LST VA VIIRS LST THÌ CÓ THỂ LẤY MEAN TRONG KHOẢNG +-2 NGÀY SO VỚI NGÀY LÀM MỐC, CÒN VỚI LANDSAT 8 LST - ĐẢM BẢO SCENE THU ĐƯỢC CÓ NGÀY GIỐNG VỚI NGÀY TỪ SENTINEL2 VÌ ĐÂY LÀ DỮ LIỆU KIỂM ĐỊNH - nếu cùng ngày mà khôgn có dữ liệu ảnh landsat thì lưu lại/in ra log để sau này dễ kiểm tra 
GEE Collection ID: COPERNICUS/S2_SR_HARMONIZED
Độ phân giải: 10m
Bộ lọc chất lượng ảnh (Cloud Masking): * Sử dụng kênh QA60 (hoặc dải phân loại lớp phủ SCL) để lọc bỏ hoàn toàn pixel mây và mây tích / bóng mây. Tham khảo scrip
Lọc điều kiện cảnh ảnh: CLOUDY_PIXEL_PERCENTAGE < 15.
Thuật toán xử lý trên GEE, thêm các hàm xử lý để thu thaapj
NDVI
EVI: $2.5 \times \frac{B8 - B4}{B8 + 6 \times B4 - 7.5 \times B2 + 1}$.
LSE ($\epsilon$ - Độ phát xạ bề mặt): Áp dụng phương pháp phân ngưỡng NDVI (NDVI Thresholds Method) dựa trên giá trị NDVI 10m vừa tính.
Các dải phổ cần trích xuất cuối cùng: ['NDVI', 'EVI', 'LSE', 'B11'] (Trong đó B11 là kênh SWIR 1 phục vụ ghi nhận cấu trúc đô thị và độ ẩm vật liệu).
// Tham khảo Cấu hình bộ lọc Metadata Sentinel-2 tối ưu cho khu vực nhiều mây
var filteredCollection = ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
  .filterBounds(roiBoundingRectangle)
  .filterDate(startDate, endDate)
  // 1. Bộ lọc mây và bóng mây (Thỏa hiệp động cho Hà Nội / Hải Phòng)
  .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 15.0))
  .filter(ee.Filter.lt('CLOUDY_SHADOW_PERCENTAGE', 10.0))
  .filter(ee.Filter.lt('THIN_CIRRUS_PERCENTAGE', 15.0))
  
  // 2. Bộ lọc kiểm soát lỗi vật lý (Nghiêm ngặt)
  .filter(ee.Filter.lt('SATURATED_DEFECTIVE_PIXEL_PERCENTAGE', 2.0))
  .filter(ee.Filter.lt('NODATA_PIXEL_PERCENTAGE', 10.0))
  .filter(ee.Filter.lt('DEGRADED_MSI_DATA_PERCENTAGE', 1.0))
  
  // 3. Bộ lọc kiểm tra chất lượng xử lý của ESA
  .filter(ee.Filter.eq('FORMAT_CORRECTNESS', 'PASSED'))
  .filter(ee.Filter.eq('GEOMETRIC_QUALITY', 'PASSED'))
  .filter(ee.Filter.eq('RADIOMETRIC_QUALITY', 'PASSED'));
2. Bộ dữ liệu nhiệt độ mịn mốc tham chiếu: Landsat 8/9 Level-2
GEE Collection ID: LANDSAT/LC08/C02/T1_L2 và LANDSAT/LC09/C02/T1_L2.
Khung giờ bay qua: Sáng (~10h30 Giờ địa phương).
Độ phân giải gốc: $100\text{ m}$ (USGS đã nội suy sẵn về lưới 30m trong sản phẩm).
Bộ lọc chất lượng ảnh: Sử dụng dải QA_PIXEL để tạo mặt nạ loại bỏ pixel mây, bóng mây. Lọc điều kiện nhiễu mây → < 5% và < 15% (lưu thành 2 band riêng biệt
Thuật toán xử lý trên GEE:
Trích xuất dải phổ nhiệt độ bề mặt xử lý sẵn: ST_B10 (Surface Temperature Band 10).
Công thức quy đổi vật lý: Nhân hệ số scale factor 0.00341802 và cộng số bù 149.0 để đưa giá trị số nguyên (DN) về đơn vị Kelvin ($K$) thực tế.
Tiếp tục trừ 273.15 để đưa ma trận về đơn vị độ Celsius (°C) theo yêu cầu thiết lập hệ thống.
Kênh trích xuất cuối cùng: ['ST_B10_Celsius'] (Đóng vai trò làm Nhãn - Ground Truth $L_0$ trong quá khứ để tính hàm lỗi). Tham khảo filter lọc dữ liệu
function updateMaskWithRadsatAndAerosol(image) {
  // 1. Lấy mặt nạ mây hiện tại từ QA_PIXEL (đã tối ưu ở bước trước)
  var imageMasked = maskLandsatSR(image); 
  
  var radsat = image.select('QA_RADSAT');
  var aerosol = image.select('SR_QA_AEROSOL');


  // ---- XỬ LÝ QA_RADSAT ----
  // Kiểm tra bão hòa các kênh quan trọng: Band 5 (bit 4), Band 6 (bit 5), Band 7 (bit 6), Band 9 (bit 8)
  // Và che khuất địa hình (bit 11)
  var satBits = (1 << 4) | (1 << 5) | (1 << 6) | (1 << 8) | (1 << 11);
  // Các bit này bắt buộc phải CHƯA ĐẶT (bằng 0)
  var radsatMask = radsat.bitwiseAnd(satBits).eq(0);


  // ---- XỬ LÝ SR_QA_AEROSOL ----
  var fillAerosol = 1 << 0;
  var baseAerosolMask = aerosol.bitwiseAnd(fillAerosol).eq(0); // Không chứa pixel trống


  // Trích xuất Bit 6-7 để lấy mức độ Aerosol (Dịch phải 6 bit và lấy giá trị 2 bit cuối)
  var aerosolLevel = aerosol.rightShift(6).bitwiseAnd(3);
  
  // THỎA HIỆP: Chỉ loại bỏ khi mức độ aerosol là CAO (giá trị = 3). Giữ lại Thấp (1) và Vừa (2).
  var aerosolConfidenceMask = aerosolLevel.lt(3);
  
  var finalAerosolMask = baseAerosolMask.and(aerosolConfidenceMask);


  // ---- KẾT HỢP TẤT CẢ MẶT NẠ ----
  return imageMasked.updateMask(radsatMask).updateMask(finalAerosolMask);
}

Lưu thêm thông tin Log để phục vụ đánh giá trọng số Loss xuất dữ liệu và tạo file log, việc thỏa hiệp chấp nhận pixel có độ nhiễm mạch khí dung mức "Vừa" (Medium) cần được ghi nhận lại để xử lý ở bước huấn luyện mô hình sau đó:Bổ sung trường vào File Log: Thêm cột medium_aerosol_pixel_ratio (Tỷ lệ pixel nhiễm khí dung mức vừa trong ảnh).Chiến lược tối ưu Loss Function: Những ảnh tháng nào có tỷ lệ medium_aerosol_pixel_ratio > 30% (thường rơi vào tháng 11 đến tháng 2 năm sau ở Hà Nội), khi đưa vào kiến trúc mạng THSTNet để tính hàm lỗi $RMSE$, ta sẽ nhân với một trọng số phạt (Penalty Weight $\alpha = 0.8$). Điều này giúp mô hình vẫn học được cấu trúc không gian đô thị của Hà Nội/Hải Phòng từ các tấm ảnh đó nhưng không bị thuật toán ép phải tối ưu tuyệt đối theo những giá trị nhiệt bị suy giảm do sương mù khí quyển.


3. Bộ dữ liệu nhiệt độ vĩ mô và xu hướng năng lượng: MODIS & VIIRS Daily LST - tham khảo bộ lọc hiện tại trong phần filters (/Users/anh/Projects/wuhi/download/src/filters/lst_filters.py)
Khung giờ Sáng (~10h30 Giờ địa phương - Phục vụ ảnh thô tham chiếu $M_0$):
GEE Collection ID: MODIS/061/MOD21A1D (Sản phẩm Daily từ vệ tinh Terra).
Kênh dữ liệu cần lấy: LST_Day_1km, QC, emissity band,...
Khung giờ Trưa (~13h30 Giờ địa phương - Phục vụ ảnh thô mục tiêu cần hạ quy mô $M_1$):
GEE Collection ID: MODIS/061/MYD21A1D (Vệ tinh Aqua) và NASA/VIIRS/002/VNP21A1D (Vệ tinh Suomi-NPP).
Kênh dữ liệu cần lấy: LST_Day_1km.
Độ phân giải 1km

VÍ DỤ CHO MODIS - CỐ GẮNG LẤY FILTER TƯƠNG TỰ CHO VIIRS VNP21A1D
function maskModisLST(image) {
  var lst = image.select('LST_Day_1km');
  var qc = image.select('QC'); // Dải kiểm soát chất lượng 16-bit

  // 1. Kiểm tra Cờ QA bắt buộc (Bit 0-1) và Chất lượng L1B (Bit 2-3)
  var qaMandatory = qc.bitwiseAnd(3);          // Lấy 2 bit cuối (0-1)
  var dataQuality = qc.rightShift(2).bitwiseAnd(3); // Dịch 2 bit, lấy 2 bit kế tiếp (2-3)
  
  // Điều kiện: QA bắt buộc phải <= 1 VÀ Chất lượng dữ liệu phải là 0 hoặc 2 (không lấy 1 và 3)
  var baseValid = qaMandatory.lte(1).and(dataQuality.eq(0).or(dataQuality.eq(2)));

  // 2. Kiểm tra Cờ Đám mây (Bit 4-5) - Tuyệt đối không lấy rìa mây (mức 2) hay mây (mức 3)
  var cloudFlag = qc.rightShift(4).bitwiseAnd(3);
  var cloudMask = cloudFlag.eq(0); // Chỉ lấy giá trị 0 (Không có mây)

  // 3. Kiểm tra Độ chính xác LST (Bit 14-15)
  var lstAccuracy = qc.rightShift(14).bitwiseAnd(3);
  // THỎA HIỆP: Loại bỏ mức 0 (Sai số > 2K). Giữ lại mức 1, 2, 3 (Sai số < 2K).
  var accuracyMask = lstAccuracy.gte(1);

  // Kết hợp tất cả các mặt nạ chất lượng
  var finalQAMask = baseValid.and(cloudMask).and(accuracyMask);

  // Trích xuất giá trị vật lý (Scale factor của MOD21A1D LST là 0.02 để về độ Kelvin)
  var lstKelvin = lst.updateMask(finalQAMask).multiply(0.02);
  
  // Chuyển đổi sang độ Celsius để đồng bộ với Landsat
  var lstCelsius = lstKelvin.subtract(273.15).rename('MODIS_LST_Celsius');

  return image.addBands(lstCelsius);
}

🎯 Lưu thêm 1 band thể hiện "Mức sai số" LST trong 1 band qc để làm thành thành Trọng số huấn luyện (Loss Weighting)
Thay vì đối xử công bằng với tất cả các pixel được giữ lại, bạn có thể tận dụng chính giá trị của Bit 14-15 để làm một ma trận trọng số (Weight Matrix) khi tính toán hàm loss của THSTNet:
Pixel có Bit 14-15 = 3 (Sai số $<1\text{K}$): Gán trọng số huấn luyện $W = 1.0$.
Pixel có Bit 14-15 = 2 (Sai số $1-1.5\text{K}$): Gán trọng số huấn luyện $W = 0.8$.
Pixel có Bit 14-15 = 1 (Sai số $1.5-2\text{K}$): Gán trọng số huấn luyện $W = 0.5$.
