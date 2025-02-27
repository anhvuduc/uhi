#!/usr/bin/env python
import ee
import rasterio
import glob, os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from airflow.models import Variable

# Initialize Earth Engine
# mình cần edit script này để dùng một image mean có sẵn, và chạy script tải mean image ở một script khác?
# Initialize Earth Engine
SERVICE_ACCOUNT = 'ducanh@ee-docxducanh.iam.gserviceaccount.com'
key_file = Variable.get('key_path', default_var='/opt/airflow/key.json')

ee_credentials = ee.ServiceAccountCredentials(SERVICE_ACCOUNT, key_file)
ee.Initialize(ee_credentials, project='ee-docxducanh')

def count_total(image_collection, data_band, qc_band, start_date, end_date, roi):
    """
    Count total pixels in ROI using a mean image from Earth Engine.
    """
    mean_img = ee.ImageCollection(image_collection) \
                 .select(data_band, qc_band) \
                 .filterDate(start_date, end_date) \
                 .mean()
    total_pixels = mean_img.reduceRegion(
        reducer=ee.Reducer.count(),
        geometry=roi,
        scale=1000,
        maxPixels=1e13
    ).getInfo()[data_band]
    return total_pixels

def count_valid(raster):
    """
    Count the number of valid pixels in a raster.
    Valid pixels: not equal to 0, not equal to -1000, and not NaN.
    """
    with rasterio.open(raster) as src:
        data = src.read(1)
        valid_pixels = np.count_nonzero((data != 0) & (data != -1000) & ~np.isnan(data))
    return valid_pixels

def main():

    # Retrieve parameters from Airflow Variables
    image_collection = Variable.get('image_collection', default_var='MODIS/061/MYD11A2')
    data_band = Variable.get('data_band', default_var='LST_Day_1km')
    qc_band = Variable.get('qc_band', default_var='QC_Day')
    start_date = Variable.get('start_date', default_var='2024-01-01')
    end_date = Variable.get('end_date', default_var='2025-01-01')
    city = Variable.get('city', default_var='hanoi')
    raw_folder = f'{image_collection.split("/")[-1]}_{data_band}_raw'.lower()
    # Define paths for input and output files (adjust these as needed)
    crop_path = f'/app/data/{city}/{image_collection.split("/")[-1]}_{data_band}_crop/'  
    quality_path = f'/app/data/{city}/{image_collection.split("/")[-1]}_{data_band}_quality/'.lower()
    if not os.path.exists(quality_path):
        os.makedirs(quality_path)
    # Folder where exported .tif files are stored
    output_csv = f'{quality_path}/quality.csv'   # Path to save the quality CSV
    output_png = f'{quality_path}/quality.png'   # Path to save the quality plot

    
    # Build the ROI using the city name
    roi = ee.FeatureCollection(f'users/anhvuduc/{city}')
    
    # Count total pixels in ROI using the mean image from Earth Engine
    total_pixels = count_total(
        image_collection,
        data_band,
        qc_band,
        start_date,
        end_date,
        roi
    )
    print("Total pixels in ROI:", total_pixels)
    
    # Gather list of TIF files from the folder.
    files_path = []
    for file_name in glob.glob(crop_path + '*.tif'):
        f = file_name.replace('\\', '/')
        files_path.append(f)
    
    # Compute the valid pixel percentage for each TIF file.
    valid_percent = []
    for file_name in files_path:
        tif_file = glob.glob(file_name)
        valid = count_valid(tif_file[0])
        percent = valid / total_pixels * 100
        valid_percent.append([(file_name.split('/')[-1]).split('.')[0], percent])
    
    df = pd.DataFrame(valid_percent, columns=['date', 'percent'])
    df.to_csv(output_csv, index=False)
    
    # Create a quality plot.
    plt.figure(figsize=(12, 8))
    plt.plot(df['date'], df['percent'], marker='o', label='Valid Pixel Percentage')
    plt.xlabel('Image Filename')
    plt.ylabel('Valid Pixel Percentage (%)')
    plt.title('Valid Pixel Percentage by QA Mask')
    plt.xticks(rotation=90)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_png)
    
    print("Quality check completed.")
    print("CSV saved at:", output_csv)
    print("Quality chart saved at:", output_png)

if __name__ == '__main__':
    main()