import ee
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'airflow',
    # 'start_date': datetime(2025, 3, 12),
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    'export_earth_engine_dynamic',
    default_args=default_args,
    schedule_interval='@None',  # Adjust schedule as needed
    catchup=False,
)

def export_task(city, image_collection, data_band, qc_band, start_date, end_date, **kwargs):
    # Static credentials (or these could also be parameterized)
    SERVICE_ACCOUNT = 'ducanh@ee-docxducanh.iam.gserviceaccount.com'
    key_file = Variable.get('key_path', default_var='/opt/airflow/key.json')
    # Create a folder name using the dataset and band
    raw_folder = f'{image_collection.split("/")[-1]}_{data_band}_raw'.lower()

    # Initialize Earth Engine
    ee_credentials = ee.ServiceAccountCredentials(SERVICE_ACCOUNT, key_file)
    ee.Initialize(ee_credentials, project='ee-docxducanh')
    print(f'Authenticated with service account: {SERVICE_ACCOUNT}')
    
    # Define ROI and folder name on Drive
    roi = ee.FeatureCollection(f'users/anhvuduc/{city}')
    print(f"Processing city: {city} in folder: {raw_folder}")

    # Helper functions for processing
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
    imgs = ee.ImageCollection(image_collection) \
              .select(data_band, qc_band) \
              .filterDate(start_date, end_date)
    masked_imgs = imgs.map(apply_mask)
    masked_imgs_list = masked_imgs.toList(masked_imgs.size())
    num_images = masked_imgs_list.size().getInfo()
    
    print(f"Total number of images to export: {num_images}")
    
    # Loop over images and export each to Google Drive
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

# Define the parameter lists
cities = ['hanoi', 'haiphong', 'danang', 'hcm']
image_collections = ['MODIS/061/MOD11A2', 'MODIS/061/MYD11A2']
data_bands = ['LST_Day_1km', 'LST_Night_1km']
qc_bands = ['QC_Day', 'QC_Night']
start_date = '2002-01-01'
end_date = '2026-01-01'

# Create tasks dynamically for each combination
for city in cities:
    for image_collection in image_collections:
        for idx, data_band in enumerate(data_bands):
            qc_band = qc_bands[idx]
            # Create a unique task_id using city, dataset, and band
            task_id = f"export_{city}_{image_collection.split('/')[-1]}_{data_band}"
            export_task = PythonOperator(
                task_id=task_id,
                python_callable=export_task,
                op_kwargs={
                    'city': city,
                    'image_collection': image_collection,
                    'data_band': data_band,
                    'qc_band': qc_band,
                    'start_date': start_date,
                    'end_date': end_date,
                },
                dag=dag,
            )
            export_task
