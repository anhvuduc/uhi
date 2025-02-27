#!/usr/bin/env python
import os
import glob
import numpy as np
import rasterio
from airflow.models import Variable

def block(month_path, block_path, start_year=2003, end_year=2024):
    """
    Reads monthly raster files from `month_path`, stacks them into a 3D array,
    computes monthly means (ignoring nodata values), fills missing pixels in each band
    with the corresponding monthly mean, and writes the merged block raster to `block_path`.
    
    Parameters:
      month_path (str): Path to the folder containing monthly rasters.
      block_path (str): Output folder for the merged block raster.
      ct (str): Product code or identifier for naming the output file.
      start_year (int): The first year in the time series.
      end_year (int): The last year in the time series.
    """
    stacked_rasters = []
    
    # Loop over the given years and months
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            file_pattern = os.path.join(month_path, f"{year}_{str(month).zfill(2)}.tif")
            files = sorted(glob.glob(file_pattern))
            if files:
                # Use the first file found (log a warning if more than one exists)
                if len(files) > 1:
                    print(f"Warning: Multiple files found for {year}_{str(month).zfill(2)}. Using {files[0]}")
                with rasterio.open(files[0]) as src:
                    arr = src.read(1).astype(np.float64)
                # Replace nodata value (-1000) with np.nan for processing
                arr[arr == -1000] = np.nan
                # Expand dims so that shape becomes [1, height, width]
                stacked_rasters.append(np.expand_dims(arr, axis=0))
    
    if not stacked_rasters:
        print("No monthly rasters found in", month_path)
        return
    
    # Stack arrays along the first axis: shape = [nband, height, width]
    stack = np.vstack(stacked_rasters)
    nband = stack.shape[0]
    print(f"Stacked {nband} monthly rasters.")
    
    # Open a sample file to get metadata (assumes file '2003_01.tif' exists)
    sample_file = os.path.join(month_path, "2025_01.tif")
    with rasterio.open(sample_file) as src:
        profile = src.meta.copy()
    
    # Compute monthly means (one per month index 0..11) using np.nanmean
    monthly_means = np.empty((12, profile['height'], profile['width']), dtype=np.float64)
    for m in range(12):
        indices = np.where(np.arange(nband) % 12 == m)[0]
        if indices.size > 0:
            monthly_means[m] = np.nanmean(stack[indices, :, :], axis=0)
        else:
            monthly_means[m] = np.nan  # No data for this month
    
    # Fill missing pixels in each band with the corresponding monthly mean
    for b in range(nband):
        m = b % 12
        band = stack[b, :, :]
        mask = np.isnan(band)
        band[mask] = monthly_means[m][mask]
        stack[b, :, :] = band
    
    # Replace any remaining NaN with the nodata value (-1000)
    stack = np.where(np.isnan(stack), -1000, stack)
    
    # Update metadata
    profile.update({
        "driver": "GTiff",
        "count": nband,
        "dtype": "float64",
        "compress": "lzw",
        "nodata": -1000
    })
    
    os.makedirs(block_path, exist_ok=True)
    merged_block_file = os.path.join(block_path, "block.tif")
    with rasterio.open(merged_block_file, "w", **profile) as dst:
        dst.write(stack)
    print("Merged block raster saved to:", merged_block_file)

if __name__ == "__main__":
    # Retrieve parameters from Airflow Variables
    city = Variable.get("city", default_var="hanoi")
    image_collection = Variable.get("image_collection", default_var="MODIS/061/MYD11A2")
    data_band = Variable.get("data_band", default_var="LST_Day_1km")
    
    # Define paths for monthly rasters and block output
    raw_folder = f"{image_collection.split('/')[-1]}_{data_band}".lower()
    # Assuming monthly rasters are stored in /app/data/<raw_folder>/month/
    month_path = f"/app/data/{city}/{raw_folder}_month/"
    # Merged block raster will be saved in /app/data/<raw_folder>/block/
    block_path = f"/app/data/{city}/{raw_folder}_block/"
    
    block(month_path, block_path, start_year=2024, end_year=2025)
