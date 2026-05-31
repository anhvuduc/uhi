# requirements: rasterio, pandas, numpy, matplotlib, seaborn, scikit-learn, scipy, tqdm

import os
import rasterio
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from tqdm import tqdm
from scipy.stats import skew, kurtosis

# ==============================================================================
# 1. CẤU HÌNH PHÂN TÍCH (QUAN TRỌNG: CHỈNH SỬA TẠI ĐÂY)
# ==============================================================================

# Đường dẫn gốc tới thư mục 'raw' 
DATA_ROOT_DIR = '/Users/anh/Projects/wuhi/analyze/data/raw' 
OUTPUT_DIR = '/Users/anh/Projects/wuhi/analyze/data/stats/threshold'

# Danh sách các thành phố (tên folder city)
CITIES = ['hanoi', 'haiphong', 'danang', 'hcm']

# Khung thời gian
START_DATE = '2000-01-01'
END_DATE = '2026-01-01'

# --- ĐỊNH NGHĨA CÁC CẶP SO SÁNH ---
# Bạn có thể thêm/bớt tùy ý. 
# 'folder': Phải khớp chính xác tên folder trong screenshot.
# 'label': Tên hiển thị trên biểu đồ.
COLLECTIONS_TO_ANALYZE = [
    {'folder': 'MOD21A1D', 'label': 'Terra (Day)', 'group': 'Day'},
    {'folder': 'MYD21A1D', 'label': 'Aqua (Day)',  'group': 'Day'},
    {'folder': 'VNP21A1D', 'label': 'VIIRS (Day)', 'group': 'Day'},
    
    {'folder': 'MOD21A1N', 'label': 'Terra (Night)', 'group': 'Night'},
    {'folder': 'MYD21A1N', 'label': 'Aqua (Night)',  'group': 'Night'},
    {'folder': 'VNP21A1N', 'label': 'VIIRS (Night)', 'group': 'Night'},
]

# ==============================================================================
# 2. HÀM XỬ LÝ
# ==============================================================================

def get_valid_lst_pixels(file_path):
    try:
        with rasterio.open(file_path) as src:
            lst_band = src.read(1).astype(np.float32)
            if src.nodata is not None:
                lst_band = np.where(lst_band == src.nodata, np.nan, lst_band)
            valid = lst_band[~np.isnan(lst_band)]
            if valid.size > 0:
                return valid, np.min(valid), np.max(valid)
            return valid, np.nan, np.nan
    except:
        return np.array([]), np.nan, np.nan

def calculate_stats(pixel_array, collection_cfg, city, filter_type, p05=np.nan, p995=np.nan):
    if pixel_array.size == 0: return None
    return {
        'City': city,
        'Collection': collection_cfg['label'],
        'Folder': collection_cfg['folder'],
        'Time_Group': collection_cfg['group'],
        'Filter_Type': filter_type,
        'Pixel Count': pixel_array.size,
        'Mean': np.mean(pixel_array),
        'Median': np.median(pixel_array),
        'Std Dev': np.std(pixel_array),
        'Min': np.min(pixel_array),
        'Max': np.max(pixel_array),
        'Skewness': skew(pixel_array),
        'Kurtosis': kurtosis(pixel_array),
        'Threshold_Low': p05,
        'Threshold_High': p995
    }

def visualize_all_collections(data_dict, output_folder, city_name):
    """
    Vẽ tất cả các datasets lên cùng 1 biểu đồ.
    data_dict: { 'Label': numpy_array_filtered }
    """
    plt.figure(figsize=(15, 8))
    
    # Định nghĩa màu sắc (Day: Gam nóng, Night: Gam lạnh)
    colors_day = ['#d62728', '#ff7f0e', '#8c564b'] # Đỏ, Cam, Nâu
    colors_night = ['#1f77b4', '#9467bd', '#17becf'] # Xanh, Tím, Cyan
    
    day_idx, night_idx = 0, 0

    print(f"  > Đang vẽ biểu đồ tổng hợp cho {city_name}...")

    for cfg in COLLECTIONS_TO_ANALYZE:
        label = cfg['label']
        if label not in data_dict: continue # Bỏ qua nếu không có dữ liệu
        
        pixels = data_dict[label]
        
        # Chọn màu
        if cfg['group'] == 'Day':
            c = colors_day[day_idx % len(colors_day)]
            ls = '-' # Nét liền cho ban ngày
            day_idx += 1
        else:
            c = colors_night[night_idx % len(colors_night)]
            ls = '--' # Nét đứt cho ban đêm
            night_idx += 1

        # Lấy mẫu để vẽ cho nhanh
        sample_size = 500_000
        if pixels.size > sample_size:
            pixels = np.random.choice(pixels, sample_size, replace=False)

        sns.kdeplot(pixels, color=c, linestyle=ls, label=label, linewidth=2, alpha=0.8)

    plt.title(f'Phân phối nhiệt độ bề mặt (LST) đa cảm biến - {city_name.upper()}', fontsize=16)
    plt.xlabel('Nhiệt độ (℃)', fontsize=12)
    plt.ylabel('Mật độ phân phối', fontsize=12)
    plt.legend(title='Bộ dữ liệu', fontsize=10, title_fontsize=12)
    plt.grid(True, linestyle=':', alpha=0.6)
    
    out_path = output_folder / f"{city_name}_lst_distribution_99.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    plt.show()
    print(f"  > Đã lưu biểu đồ: {out_path}")

# ==============================================================================
# 3. MAIN
# ==============================================================================

def main():
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    date_range = pd.date_range(START_DATE, END_DATE, freq='MS')
    all_stats_list = []

    print(f"Bắt đầu phân tích tổng hợp cho {len(CITIES)} thành phố...")

    for city in tqdm(CITIES, desc="Processing Cities"):
        city_out_dir = Path(OUTPUT_DIR) / city
        city_out_dir.mkdir(parents=True, exist_ok=True)
        
        # Dictionary để lưu dữ liệu filtered dùng cho việc vẽ biểu đồ sau cùng
        # Key: Label, Value: Filtered Pixels Array
        city_plot_data = {} 

        # Duyệt qua từng bộ sưu tập (MOD Day, MOD Night, MYD Day...)
        for cfg in COLLECTIONS_TO_ANALYZE:
            folder_name = cfg['folder']
            
            # 1. Thu thập pixels
            pixels_list = []
            for dt in date_range:
                fpath = Path(DATA_ROOT_DIR) / city / folder_name / f"{dt.strftime('%Y_%m')}.tif"
                if fpath.exists():
                    p, _, _ = get_valid_lst_pixels(fpath)
                    if p.size > 0: pixels_list.append(p)
            
            raw_pixels = np.concatenate(pixels_list) if pixels_list else np.array([])
            
            if raw_pixels.size == 0:
                continue # Không có dữ liệu thì bỏ qua

            # 2. Tính Stats RAW
            s_raw = calculate_stats(raw_pixels, cfg, city, 'Raw (Extreme)')
            all_stats_list.append(s_raw)

            # 3. Lọc (Filter) & Tính Stats FILTERED
            p05, p995 = np.percentile(raw_pixels, [0.5, 99.5])
            filtered_pixels = raw_pixels[(raw_pixels >= p05) & (raw_pixels <= p995)]
            
            s_filt = calculate_stats(filtered_pixels, cfg, city, 'Filtered (P0.5-P99.5)', p05, p995)
            all_stats_list.append(s_filt)

            # 4. Lưu dữ liệu sạch vào dict để lát nữa vẽ
            city_plot_data[cfg['label']] = filtered_pixels

        # 5. Vẽ biểu đồ chung cho thành phố này (nếu có dữ liệu)
        if city_plot_data:
            visualize_all_collections(city_plot_data, city_out_dir, city)

    # --- Xuất CSV tổng ---
    if all_stats_list:
        df = pd.DataFrame(all_stats_list)
        # Sắp xếp cột
        cols = ['City', 'Collection', 'Time_Group', 'Filter_Type', 'Pixel Count', 
                'Mean', 'Median', 'Std Dev', 'Min', 'Max', 'Skewness', 'Kurtosis', 
                'Threshold_Low', 'Threshold_High']
        df = df[[c for c in cols if c in df.columns]]
        
        csv_path = Path(OUTPUT_DIR) / "lst_distribution_99.csv"
        df.to_csv(csv_path, index=False, float_format='%.3f')
        print(f"\nHoàn tất! File thống kê tổng hợp: {csv_path}")
    else:
        print("Không có dữ liệu nào được xử lý.")

if __name__ == '__main__':
    main()