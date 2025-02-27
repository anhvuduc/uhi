#!/usr/bin/env python
import os
import io
import time
import datetime
import ee
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from airflow.models import Variable

def export_image():
    """
    Tính ảnh trung bình từ Earth Engine và xuất ảnh lên Google Drive.
    Tên file và folder được đặt theo định dạng: 
      {city}_{imageCollectionName}_{dataBand}_ee_mean
    """
    # Khởi tạo Earth Engine
    # Initialize Earth Engine
    SERVICE_ACCOUNT = 'ducanh@ee-docxducanh.iam.gserviceaccount.com'
    key_file = Variable.get('key_path', default_var='/opt/airflow/key.json')
    ee_credentials = ee.ServiceAccountCredentials(SERVICE_ACCOUNT, key_file)
    ee.Initialize(ee_credentials, project='ee-docxducanh')
    
    # Lấy các biến cấu hình từ Airflow Variables
    image_collection = Variable.get('image_collection', default_var='MODIS/061/MYD11A2')
    data_band = Variable.get('data_band', default_var='LST_Day_1km')
    qc_band = Variable.get('qc_band', default_var='QC_Day')
    start_date = Variable.get('start_date', default_var='2024-01-01')
    end_date = Variable.get('end_date', default_var='2025-01-01')
    city = Variable.get('city', default_var='hanoi')
    
    # Lấy vùng quan tâm (ROI)
    roi = ee.FeatureCollection(f"users/anhvuduc/{city}")
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
    
    # Tính ảnh trung bình của collection theo khoảng thời gian khảo sát
    mean_img = ee.ImageCollection(image_collection)\
                .select(data_band, qc_band)\
                .filterDate(start_date, end_date)\
                .mean()
    mean_img = mean_img.map(apply_mask)
    # Xây dựng tên file và folder: {city}_{imageCollectionName}_{dataBand}_ee_mean
    image_collection_name = image_collection.split("/")[-1]
    export_folder = f'{city}_{image_collection_name}_{data_band}_ee_mean'.lower()
    file_prefix = export_folder  # Dùng chung cho tên file
    
    # Thiết lập và bắt đầu task export ảnh lên Google Drive
    task = ee.batch.Export.image.toDrive(
            image=mean_img,
            region=roi.geometry().bounds(),
            description=f'{file_prefix}_export',
            folder=export_folder,
            fileNamePrefix=file_prefix,
            scale=1000,
            crs='EPSG:4326',
            fileFormat='GeoTIFF'
        )
    task.start()
    print(f"{datetime.datetime.now()} - Export task '{file_prefix}_export' đã được khởi chạy.")
    
    # Poll trạng thái của task cho đến khi hoàn thành hoặc thất bại
    while True:
        status = task.status()
        state = status.get('state')
        print(f"{datetime.datetime.now()} - Trạng thái task: {state}")
        if state in ['COMPLETED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(30)  # Đợi 30 giây trước khi kiểm tra lại
    
    if state == 'COMPLETED':
        print(f"{datetime.datetime.now()} - Export task hoàn tất thành công.")
    else:
        error_message = status.get('error_message', 'Unknown error')
        print(f"{datetime.datetime.now()} - Export task không thành công. Trạng thái: {state}, Lỗi: {error_message}")
        raise Exception("Export task failed")
    
    return export_folder, file_prefix

def get_folder_id(service, folder_name, parent_folder_id=None):
    """
    Lấy folder ID từ Google Drive dựa theo tên folder.
    """
    query = f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}'"
    if parent_folder_id:
        query += f" and '{parent_folder_id}' in parents"
    
    results = service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get('files', [])
    if items:
        return items[0]['id']
    return None

def download_files(export_folder, file_prefix):
    """
    Tải file .tif từ thư mục Google Drive có tên export_folder về máy cục bộ.
    Thư mục cục bộ: /app/data/{city}/{export_folder}/
    """
    # Lấy cấu hình từ Airflow Variables
    city = Variable.get('city', default_var='hanoi')
    image_collection = Variable.get('image_collection', default_var='MODIS/061/MYD11A2')
    data_band = Variable.get('data_band', default_var='LST_Day_1km')
    key_file = Variable.get('key_path', default_var='/opt/airflow/key.json')
    
    # Xác định folder trên Google Drive (đã được đặt khi export)
    raw_folder = export_folder
    
    # Xây dựng thư mục đích trên máy cục bộ (ví dụ trong Docker Airflow)
    destination_dir = f'/app/data/{city}/{image_collection.split('/')[-1]}_{data_band}_mean/'
    os.makedirs(destination_dir, exist_ok=True)
    
    # Xác thực Google Drive API
    credentials = service_account.Credentials.from_service_account_file(
        key_file, scopes=['https://www.googleapis.com/auth/drive']
    )
    service = build('drive', 'v3', credentials=credentials)
    
    # Lấy folder ID từ Google Drive
    folder_id = get_folder_id(service, raw_folder)
    if folder_id:
        print(f"Folder ID của '{raw_folder}': {folder_id}")
    else:
        raise Exception(f"Không tìm thấy folder '{raw_folder}' trên Google Drive.")
    
    # Lấy danh sách các file trong folder (loại trừ file bị xóa)
    results = service.files().list(
        q=f"'{folder_id}' in parents and trashed = false",
        fields="files(id, name, mimeType)"
    ).execute()
    items = results.get('files', [])
    
    if not items:
        raise Exception(f"Không tìm thấy file nào trong folder có ID: {folder_id}")
    else:
        print(f"Đã tìm thấy {len(items)} file trong folder có ID: {folder_id}")
    
    # Tải các file có đuôi .tif (bỏ qua các thư mục con)
    for item in items:
        file_id = item['id']
        file_name = item['name']
        mime_type = item['mimeType']
        
        if mime_type == 'application/vnd.google-apps.folder':
            print(f"Bỏ qua thư mục con: {file_name} (ID: {file_id})")
            continue
        if not file_name.lower().endswith('.tif'):
            print(f"Bỏ qua file không phải .tif: {file_name}")
            continue
        
        print(f"Đang tải file: {file_name} (ID: {file_id})")
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                print(f"Đã tải {int(status.progress() * 100)}% file {file_name}.")
        destination_path = os.path.join(destination_dir, file_name)
        with open(destination_path, 'wb') as f:
            f.write(fh.getvalue())
        print(f"File {file_name} đã được tải về {destination_path}")

def main():
    try:
        # Bước 1: Xuất ảnh từ Earth Engine và chờ hoàn thành task
        export_folder, file_prefix = export_image()
        # Bước 2: Tải file từ Google Drive về máy cục bộ
        download_files(export_folder, file_prefix)
    except Exception as e:
        print("Có lỗi xảy ra trong quá trình thực hiện:", e)

if __name__ == '__main__':
    main()