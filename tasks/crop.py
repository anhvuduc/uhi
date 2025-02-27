#!/usr/bin/env python
import os
import glob
import ntpath
import fiona
import rasterio
import rasterio.mask as msk
from airflow.models import Variable

def crop_rasters():
    # Retrieve parameters from Airflow Variables
    city = Variable.get('city', default_var='hanoi')
    # Path to the shapefile used for cropping; e.g., /opt/airflow/shp/hanoi/hanoi.shp
    shp_path = Variable.get('shp_path', default_var=f'/opt/airflow/shp/{city}/{city}.shp')
    # Folder containing the downloaded TIF files (from the download task)
    image_collection = Variable.get('image_collection', default_var='MODIS/061/MYD11A2')
    data_band = Variable.get('data_band', default_var='LST_Day_1km')
    raw_folder = f'{image_collection.split('/')[-1]}_{data_band}_raw'.lower()

    raw_path =  f'/app/data/{city}/{raw_folder}/'
    # Folder where cropped files will be saved; e.g. /opt/airflow/hanoi_myd11a2_lst_day_1km_crop
    crop_path = f'/app/data/{city}/{image_collection.split('/')[-1]}_{data_band}_crop/'.lower()
    
    # Ensure the crop folder exists
    if not os.path.exists(crop_path):
        os.makedirs(crop_path)
    
    # Open the shapefile with fiona and extract geometries
    with fiona.open(shp_path, 'r') as shp:
        shapes = [feature['geometry'] for feature in shp]
    
    # Get all TIF files from the download folder
    tif_files = glob.glob(os.path.join(raw_path, '*.tif'))
    
    if not tif_files:
        print('No TIF files found in the download folder.')
        return

    for tif_file in tif_files:
        try:
            with rasterio.open(tif_file) as src:
                profile = src.meta.copy()
                # Crop the raster using the shapefile geometries
                cropped_raster, out_trans = msk.mask(src, shapes, crop=True, nodata=-1000, all_touched=False)
                # Create a mask for valid data (assuming valid pixels are >= 0)
                valid_mask = cropped_raster >= 0
                cropped_raster = cropped_raster * valid_mask
                # Update metadata for the cropped raster
                profile.update({
                    'driver': 'GTiff',
                    'transform': out_trans,
                    'height': cropped_raster.shape[1],
                    'width': cropped_raster.shape[2],
                    'count': 1,
                    'dtype': rasterio.float32,
                    'compress': 'lzw',
                    'nodata': -1000
                })
                origin_file_name = ntpath.basename(tif_file)
                dst_file_name = os.path.join(crop_path, origin_file_name)
                print(dst_file_name)
                with rasterio.open(dst_file_name, 'w', **profile) as dst:
                    dst.write(cropped_raster)
                print(f'Cropped file saved: {dst_file_name}')
        except Exception as e:
            print(f'Error processing {tif_file}: {e}')

if __name__ == '__main__':
    crop_rasters()