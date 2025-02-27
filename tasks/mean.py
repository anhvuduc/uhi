# tải mean picityure 20 năm theo công thức của earth engine xong so sánh với kết quả mean của mình
# thực hiện tính trung bình tháng cho ảnh đã tải về trong 20 năm
# 
#!/usr/bin/env python
import os
import glob
import rasterio
import numpy as np
from airflow.models import Variable


# ------------------------
# Approach 2: Compute Mean from Local Cropped TIF Files
# ------------------------

def overall_mean(crop_path, mean_path, city, start_year, end_year):
    '''
    Reads all TIF files from crop_path, computes the overall mean (ignoring nodata),
    and writes the resulting raster to mean_path.
        '''
    # Lấy tất cả file .tif trong crop_path
    # file_list = sorted(glob.glob(os.path.join(crop_path, '*.tif')))

    # Lọc các file có năm nằm trong khoảng từ start_year đến end_year
    file_list = [
        f for f in sorted(glob.glob(os.path.join(crop_path, '*.tif')))
        if start_year <= int(os.path.basename(f).split('_')[0]) <= end_year
    ]
    if not file_list:
        print('No TIF files found in:', crop_path)
        return
    
    # Open the first file to grab metadata
    with rasterio.open(file_list[0]) as src:
        meta = src.meta.copy()
    
    # Read each file into a numpy array (convert nodata -1000 to np.nan)
    arrays = []
    for f in file_list:
        with rasterio.open(f) as src:
            arr = src.read(1).astype(np.float64)
            arr[arr == -1000] = np.nan
            arrays.append(arr)
    
    # Stack arrays along a new axis and compute the mean ignoring NaN values
    stack = np.stack(arrays, axis=0)
    mean_arr = np.nanmean(stack, axis=0)
    # Replace any remaining NaN with the nodata value (-1000)
    mean_arr = np.where(np.isnan(mean_arr), -1000, mean_arr)
    
    meta.update({'count': 1, 'dtype': 'float64', 'nodata': -1000})
    os.makedirs(mean_path, exist_ok=True)
    mean_tif = os.path.join(mean_path, 'mean.tif')
    with rasterio.open(mean_tif, 'w', **meta) as dst:
        dst.write(mean_arr, 1)
    print(f'Overall mean raster saved to {mean_tif}')
# ------------------------
def monthly_mean(crop_path, month_path, start_year, end_year):
    '''
    For each year and month between start_year and end_year,
    reads all TIF files in crop_path matching the pattern,
    computes a monthly mean (ignoring nodata), and writes the output to month_path.
    '''
    os.makedirs(month_path, exist_ok=True)
    
    count = 0
    # Assume files are named like YYYY_MM_DD.tif
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            pattern = os.path.join(crop_path, f'{year}_{str(month).zfill(2)}_*.tif')
            files = sorted(glob.glob(pattern))
            if not files:
                continue
            arrays = []
            for f in files:
                with rasterio.open(f) as src:
                    arr = src.read(1).astype(np.float64)
                    arr[arr == -1000] = np.nan
                    arrays.append(arr)
            if not arrays:
                continue
            stack = np.stack(arrays, axis=0)
            mean_arr = np.nanmean(stack, axis=0)
            mean_arr = np.where(np.isnan(mean_arr), -1000, mean_arr)
            with rasterio.open(files[0]) as src:
                meta = src.meta.copy()
            meta.update({'count': 1, 'dtype': 'float64', 'nodata': -1000})
            output_file = os.path.join(month_path, f'{year}_{str(month).zfill(2)}.tif')
            with rasterio.open(output_file, 'w', **meta) as dst:
                dst.write(mean_arr, 1)
            count += 1
            print(f'Processed monthly mean: {output_file}')
    print(f'Total monthly mean rasters processed: {count}')

def main():
    # Retrieve parameters from Airflow Variables (or set defaults)
    city = Variable.get('city', default_var='hanoi')
    image_collection = Variable.get('image_collection', default_var='MODIS/061/MYD11A2')
    start_date = Variable.get('start_date', default_var='2025-01-01')
    end_date = Variable.get('end_date', default_var='2025-02-01')
    data_band = Variable.get('data_band', default_var='LST_Day_1km')
    crop_path = f'/app/data/{city}/{image_collection.split('/')[-1]}_{data_band}_crop/'

    start_year = int(start_date.split('-')[0])
    end_year = int(end_date.split('-')[0])
    # # Define output direcityories for the mean and monthly mean results
    # # Here we build paths under /opt/airflow/mean and /opt/airflow/month using the 'when' variable.
    mean_path = f'/app/data/{city}/{image_collection.split('/')[-1]}_{data_band}_mean/'.lower()
    month_path = f'/app/data/{city}/{image_collection.split('/')[-1]}_{data_band}_month/'.lower()
    
    print('Starting overall mean calculation...')
    overall_mean(crop_path, mean_path, city, start_year, end_year)
    
    print('Starting monthly mean calculation...')
    monthly_mean(crop_path, month_path, start_year, end_year)

if __name__ == '__main__':
    main()