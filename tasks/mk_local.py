#!/usr/bin/env python

import os
import csv
import numpy as np
import rasterio
import rasterio.mask
import fiona
import pymannkendall as mk
# from airflow.models import Variable

# --------------------------
# Retrieve parameters from Airflow Variables
# --------------------------
city = "hanoi" #Variable.get("city", default_var="hanoi")
image_collection = "MODIS/061/MYD11A2"#Variable.get("image_collection", default_var="MODIS/061/MYD11A2")
data_band = "LST_Day_1km" #Variable.get("data_band", default_var="LST_Day_1km")
# Build a folder name similar to block.py
raw_folder = f"{image_collection.split('/')[-1]}_{data_band}".lower()

# --------------------------
# Define input and output paths
# --------------------------
# Input block raster created by block.py (named "block.tif")
block_path =  r'D:\Project\MODIS\data\hanoi\myd11a2_lst_day_1km_block\block.tif' # f"/app/data/{city}/{raw_folder}_block/block.tif"

# Define output directory for Mann-Kendall results
mk_dir = r'D:\Project\MODIS\data\hanoi\myd11a2_lst_day_1km_mk'# f"/app/data/{city}/{raw_folder}_mk"
os.makedirs(mk_dir, exist_ok=True)
output_raster_path = os.path.join(mk_dir, "smk.tif")

# Directories for result CSV and images
result_dir = os.path.join(mk_dir, "result")
os.makedirs(result_dir, exist_ok=True)
csv_res_path = os.path.join(result_dir, "csv")
os.makedirs(csv_res_path, exist_ok=True)
img_res_path = os.path.join(result_dir, "img")
os.makedirs(img_res_path, exist_ok=True)

# Define the shapefile path for masking (update this path accordingly)
shp_path = r'D:\Project\MODIS\shp\hanoi\hanoi.shp' #f"/app/data/{city}/shapefile.shp"

# --------------------------
# Load the block raster data using rasterio (instead of GDAL)
# --------------------------
with rasterio.open(block_path) as src:
    raster_data = src.read()  # Expecting shape: [nband, rows, cols]
    nband, rows, cols = raster_data.shape
    print("Raster dimensions (bands, rows, cols):", nband, rows, cols)

# --------------------------
# Create an empty container for seasonal trend results
# --------------------------
seasonal_trend_results = np.empty((rows, cols), dtype=object)

# Function to apply the Seasonal Mann-Kendall test on a time series
def analyze_seasonal_trend(time_series):
    result = mk.seasonal_test(time_series, period=12)
    if result is None:
        return None
    trend = result.trend
    if trend == 'increasing':
        trend_code = 1
    elif trend == 'decreasing':
        trend_code = -1
    else:
        trend_code = 0
    return trend_code, result.p, result.z, result.Tau, result.slope

# --------------------------
# Calculate trend statistics for each pixel
# --------------------------
for i in range(rows):
    for j in range(cols):
        pixel_time_series = raster_data[:, i, j]
        result = analyze_seasonal_trend(pixel_time_series)
        if result is not None:
            seasonal_trend_results[i, j] = result
        else:
            seasonal_trend_results[i, j] = (np.nan, np.nan, np.nan, np.nan, np.nan)

# --------------------------
# Separate results into individual arrays
# --------------------------
trend_array = np.full((rows, cols), np.nan, dtype=float)
p_array = np.full((rows, cols), np.nan, dtype=float)
z_array = np.full((rows, cols), np.nan, dtype=float)
Tau_array = np.full((rows, cols), np.nan, dtype=float)
slope_array = np.full((rows, cols), np.nan, dtype=float)

for i in range(rows):
    for j in range(cols):
        trend, p, z, Tau, slope = seasonal_trend_results[i, j]
        trend_array[i, j] = trend
        p_array[i, j] = p
        z_array[i, j] = z
        Tau_array[i, j] = Tau
        slope_array[i, j] = slope

# --------------------------
# Apply a p-value mask (only retain statistically significant trends)
# --------------------------
mask_p = p_array > 0.05
trend_array[mask_p] = np.nan
z_array[mask_p] = np.nan
Tau_array[mask_p] = np.nan
slope_array[mask_p] = np.nan

# --------------------------
# Read metadata from the input block raster using rasterio
# --------------------------
with rasterio.open(block_path) as src:
    meta = src.meta
    transform = src.transform
    crs = src.crs

meta.update({
    'driver': 'GTiff',
    'count': 5,       # Five bands: trend, p, z, Tau, slope
    'dtype': 'float32',
    'nodata': np.nan
})

# --------------------------
# Create a mask using the shapefile
# --------------------------
with fiona.open(shp_path, 'r') as shp:
    shapes = [feature["geometry"] for feature in shp]

with rasterio.open(block_path) as src:
    out_image, out_transform = rasterio.mask.mask(src, shapes, crop=False, nodata=np.nan)
    mask_shape = out_image[0, :, :] == src.nodata

# Apply the shapefile mask to all arrays
trend_array[mask_shape] = np.nan
p_array[mask_shape] = np.nan
z_array[mask_shape] = np.nan
Tau_array[mask_shape] = np.nan
slope_array[mask_shape] = np.nan

# --------------------------
# Write the Mann-Kendall results to a new raster file
# --------------------------
with rasterio.open(output_raster_path, 'w', **meta) as dst:
    dst.write(trend_array.astype(np.float32), 1)
    dst.write(p_array.astype(np.float32), 2)
    dst.write(z_array.astype(np.float32), 3)
    dst.write(Tau_array.astype(np.float32), 4)
    dst.write(slope_array.astype(np.float32), 5)

print("Mann–Kendall raster saved to:", output_raster_path)

# --------------------------
# Function to calculate summary statistics
# --------------------------
def calculate_statistics(slope_array, transform):
    mean_slope = np.nanmean(slope_array)
    median_slope = np.nanmedian(slope_array)
    highest_slope = np.nanmax(slope_array)
    lowest_slope = np.nanmin(slope_array)

    no_trend_count = np.sum(slope_array == 0)
    increasing_trend_count = np.sum(slope_array > 0)
    decreasing_trend_count = np.sum(slope_array < 0)

    total_count = np.size(slope_array)
    valid_count = np.count_nonzero(~np.isnan(slope_array))
    no_data_count = total_count - valid_count
    percentage_no_data = (no_data_count / total_count) * 100

    percentage_no_trend = (no_trend_count / valid_count) * 100
    percentage_increasing_trend = (increasing_trend_count / valid_count) * 100
    percentage_decreasing_trend = (decreasing_trend_count / valid_count) * 100
    percentage_greater_equal_mean = (np.sum(slope_array >= mean_slope) / valid_count) * 100

    max_index = np.unravel_index(np.nanargmax(slope_array), slope_array.shape)
    min_index = np.unravel_index(np.nanargmin(slope_array), slope_array.shape)
    max_coords = rasterio.transform.xy(transform, max_index[0], max_index[1])
    min_coords = rasterio.transform.xy(transform, min_index[0], min_index[1])

    return {
        "mean_slope": mean_slope,
        "median_slope": median_slope,
        "highest_slope": highest_slope,
        "lowest_slope": lowest_slope,
        "percentage_no_data": percentage_no_data,
        "percentage_no_trend": percentage_no_trend,
        "percentage_increasing_trend": percentage_increasing_trend,
        "percentage_decreasing_trend": percentage_decreasing_trend,
        "percentage_greater_equal_mean": percentage_greater_equal_mean,
        "max_coords": max_coords,
        "min_coords": min_coords,
        "max_index": max_index,
        "min_index": min_index
    }

# --------------------------
# Calculate statistics for each shapefile feature and the overall area
# --------------------------
with rasterio.open(output_raster_path) as src:
    trend_array = src.read(1)
    p_array = src.read(2)
    z_array = src.read(3)
    Tau_array = src.read(4)
    slope_array = src.read(5)
    transform = src.transform

    results = []
    # Calculate statistics for each feature in the shapefile
    for num, shape in enumerate(shapes, start=1):
        out_image, out_transform = rasterio.mask.mask(src, [shape], crop=True)
        # Slope band is the 5th band (index 4)
        feature_slope_array = out_image[4]
        statistics = calculate_statistics(feature_slope_array, out_transform)
        statistics['area'] = f'Feature {num}'
        results.append(statistics)

    # Overall statistics for the entire raster area
    overall_statistics = calculate_statistics(slope_array, transform)
    overall_statistics['area'] = 'Whole City'
    results.append(overall_statistics)

# --------------------------
# Write the statistics to a CSV file
# --------------------------
csv_file_path = os.path.join(csv_res_path, "trend_statistics.csv")
with open(csv_file_path, mode='w', newline='') as csv_file:
    fieldnames = [
        'area', 'mean_slope', 'median_slope', 'highest_slope', 'lowest_slope',
        'percentage_no_data', 'percentage_no_trend', 'percentage_increasing_trend',
        'percentage_decreasing_trend', 'percentage_greater_equal_mean',
        'max_coords', 'min_coords', 'max_index', 'min_index'
    ]
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    for result in results:
        writer.writerow(result)

print("Trend statistics saved to:", csv_file_path)