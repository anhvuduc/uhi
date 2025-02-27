import ee
from datetime import datetime
from airflow.models import Variable

def export_task(**kwargs):
    # Set up Earth Engine credentials and Airflow Variables
    SERVICE_ACCOUNT = 'ducanh@ee-docxducanh.iam.gserviceaccount.com'
    city = Variable.get('city', default_var='hanoi')
    image_collection = Variable.get('image_collection', default_var='MODIS/061/MYD11A2')
    start_date = Variable.get('start_date', default_var='2025-01-01')
    end_date = Variable.get('end_date', default_var='2025-02-01')
    data_band = Variable.get('data_band', default_var='LST_Day_1km')
    qc_band = Variable.get('qc_band', default_var='QC_Day')
    key_file = Variable.get('key_path', default_var='/opt/airflow/key.json')
    raw_folder = f'{image_collection.split('/')[-1]}_{data_band}_raw'.lower()

    # Initialize Earth Engine
    ee_credentials = ee.ServiceAccountCredentials(SERVICE_ACCOUNT, key_file)
    ee.Initialize(ee_credentials, project='ee-docxducanh')
    print(f'Authenticated with service account: {SERVICE_ACCOUNT}')
    
    # Define ROI and folder name on Drive
    roi = ee.FeatureCollection(f'users/anhvuduc/{city}')
    print(raw_folder)  # Print folder name for logging
    
    # Helper functions
    def bitwise_extract(input, from_bit, to_bit):
        mask_size = 1 + to_bit - from_bit
        mask = ee.Number(1).leftShift(mask_size).subtract(1)
        return input.rightShift(from_bit).bitwiseAnd(mask)
    
    def apply_mask(image):
        lst_img = image.select(data_band)
        lst_img = lst_img.multiply(0.02).subtract(273.15)
        qc = image.select(qc_band)
        qa_mask = bitwise_extract(qc, 0, 1).lte(1)
        error_mask = bitwise_extract(qc, 6, 7).lte(2)
        mask = qa_mask.And(error_mask)
        return lst_img.updateMask(mask)
    
    # Process Image Collection
    imgs = ee.ImageCollection(image_collection).select(data_band, qc_band).filterDate(start_date, end_date)
    masked_imgs = imgs.map(apply_mask)
    masked_imgs_list = masked_imgs.toList(masked_imgs.size())
    num_images = masked_imgs_list.size().getInfo()
    
    # Print only the number so that BashOperator captures it in XCom
    print(num_images)
    
    # Export each image to Google Drive
    for i in range(num_images):
        img = ee.Image(masked_imgs_list.get(i))
        try:
            img_id = img.id().getInfo()
            filename = img_id.split('/')[-1]
        except Exception:
            filename = f'image_{i}'
        task = ee.batch.Export.image.toDrive(
            image=img,
            region=roi.geometry().bounds(),
            description=filename,
            folder=raw_folder,
            scale=1000,
            crs='EPSG:4326',
            fileFormat='GeoTIFF'
        )
        task.start()
        print(f'{datetime.now()} - Exporting {filename}...')
    
    return num_images

if __name__ == '__main__':
    result = export_task()
    # IMPORTANT: Print only the number for XCom (avoid extra text)
    print(result)
