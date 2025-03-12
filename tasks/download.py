#!/usr/bin/env python
import io
import os
import time  # Import time for sleep
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from airflow.models import Variable

def get_folder_id(service, folder_name, parent_folder_id=None):
    """
    Get the folder ID for a folder with the given name.
    Returns the ID of the first matching folder, or None if not found.
    """
    query = f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}'"
    if parent_folder_id:
        query += f" and '{parent_folder_id}' in parents"
    
    results = service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get('files', [])
    if items:
        return items[0]['id']
    return None

def download_task():
    # Retrieve configuration from Airflow Variables
    city = Variable.get('city', default_var='hanoi')
    image_collection = Variable.get('image_collection', default_var='MODIS/061/MYD11A2')
    data_band = Variable.get('data_band', default_var='LST_Day_1km')
    key_file = Variable.get('key_path', default_var='/opt/airflow/key.json')
    
    # Build folder name and local destination path
    raw_folder = f"{image_collection.split('/')[-1]}_{data_band}_raw".lower()
    destination_dir = os.path.join('/app/data', city, raw_folder)
    os.makedirs(destination_dir, exist_ok=True)
    
    # Authenticate with Google Drive API
    credentials = service_account.Credentials.from_service_account_file(
        key_file, scopes=['https://www.googleapis.com/auth/drive']
    )
    service = build('drive', 'v3', credentials=credentials)
    
    # Find the remote folder
    folder_id = get_folder_id(service, raw_folder)
    if folder_id:
        print(f"Folder ID for '{raw_folder}': {folder_id}")
    else:
        print(f"No folder named '{raw_folder}' was found on Drive.")
        return

    # Function to count TIF files in the remote folder
    def count_tif_files():
        results = service.files().list(
            q=f"'{folder_id}' in parents and trashed = false",
            fields="files(id, name, mimeType)"
        ).execute()
        items = results.get('files', [])
        tif_files = [
            item for item in items
            if item['mimeType'] != 'application/vnd.google-apps.folder' and item['name'].lower().endswith('.tif')
        ]
        return len(tif_files), items

    # Retrieve the expected number of images from the Airflow Variable
    expected_num = int(Variable.get('num_images', default_var='0'))
    print(f"Expected number of images: {expected_num}")
    
    # # Check the current number of TIF files
    # tif_count, items = count_tif_files()
    # print(f"Found {tif_count} TIF file(s) in folder '{raw_folder}'.")

    # # Retry up to 3 times with a 5-minute delay if not all images are present
    # retry_count = 0
    # max_retries = 3
    # while tif_count < expected_num and retry_count < max_retries:
    #     print("Not all images have been uploaded to Drive yet. Waiting for 5 minutes before checking again...")
    #     time.sleep(300)  # Wait for 5 minutes (300 seconds)
    #     retry_count += 1
    #     tif_count, items = count_tif_files()
    #     print(f"After waiting, found {tif_count} TIF file(s) in folder '{raw_folder}'.")

    # if tif_count < expected_num:
    #     print("Error: Expected number of images not reached after retries. Exiting without downloading.")
    #     return
    
    # # Proceed to download each file (only .tif files, skipping subfolders)
    # for item in items:
    #     file_id = item['id']
    #     file_name = item['name']
    #     mime_type = item['mimeType']
        
    #     if mime_type == 'application/vnd.google-apps.folder':
    #         print(f"Skipping subfolder: {file_name} (ID: {file_id})")
    #         continue
    #     if not file_name.lower().endswith('.tif'):
    #         print(f"Skipping non-TIF file: {file_name}")
    #         continue
        
    #     print(f"Downloading: {file_name} (ID: {file_id})")
    #     request = service.files().get_media(fileId=file_id)
    #     fh = io.BytesIO()
    #     downloader = MediaIoBaseDownload(fh, request)
    #     done = False
        
    #     while not done:
    #         status, done = downloader.next_chunk()
    #         if status:
    #             print(f"Download {int(status.progress() * 100)}% complete for {file_name}.")
        
    #     destination_path = os.path.join(destination_dir, file_name)
    #     with open(destination_path, 'wb') as f:
    #         f.write(fh.getvalue())
    #     print(f"Downloaded {file_name} to {destination_path}")

def main():
    try:
        download_task()
    except Exception as e:
        print("Error during download task:", e)

if __name__ == '__main__':
    main()

# # # download.py
# #!/usr/bin/env python
# import io
# import os
# import glob
# from google.oauth2 import service_account
# from googleapiclient.discovery import build
# from googleapiclient.http import MediaIoBaseDownload
# from airflow.models import Variable

# def get_folder_id(service, folder_name, parent_folder_id=None):
#     """
#     Get the folder ID for a folder with the given name.
#     Returns the ID of the first matching folder, or None if not found.
#     """
#     query = f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}'"
#     if parent_folder_id:
#         query += f" and '{parent_folder_id}' in parents"
    
#     results = service.files().list(q=query, fields="files(id, name)").execute()
#     items = results.get('files', [])
#     if items:
#         return items[0]['id']
#     return None

# def download_task():
#     # Retrieve configuration from Airflow Variables
#     city = Variable.get('city', default_var='hanoi')
#     image_collection = Variable.get('image_collection', default_var='MODIS/061/MYD11A2')
#     data_band = Variable.get('data_band', default_var='LST_Day_1km')
#     key_file = Variable.get('key_path', default_var='/opt/airflow/key.json')
    
#     # Build folder name and local destination path
#     raw_folder = f"{image_collection.split('/')[-1]}_{data_band}_raw".lower()
#     destination_dir = os.path.join('/app/data', city, raw_folder)
#     os.makedirs(destination_dir, exist_ok=True)
    
#     # Authenticate Google Drive API
#     credentials = service_account.Credentials.from_service_account_file(
#         key_file, scopes=['https://www.googleapis.com/auth/drive']
#     )
#     service = build('drive', 'v3', credentials=credentials)
    
#     # Find the remote folder
#     folder_id = get_folder_id(service, raw_folder)
#     if folder_id:
#         print(f"Folder ID for '{raw_folder}': {folder_id}")
#     else:
#         print(f"No folder named '{raw_folder}' was found on Drive.")
#         return
    
#     # List files in the remote folder (ignoring trashed files)
#     results = service.files().list(
#         q=f"'{folder_id}' in parents and trashed = false",
#         fields="files(id, name, mimeType)"
#     ).execute()
#     items = results.get('files', [])
    
#     if not items:
#         print(f"No files found in folder with ID: {folder_id}")
#         return
#     else:
#         print(f"Found {len(items)} file(s) in folder with ID: {folder_id}")
    
#     # Download each file (only .tif files, skipping subfolders)
#     for item in items:
#         file_id = item['id']
#         file_name = item['name']
#         mime_type = item['mimeType']
        
#         if mime_type == 'application/vnd.google-apps.folder':
#             print(f"Skipping subfolder: {file_name} (ID: {file_id})")
#             continue
#         if not file_name.lower().endswith('.tif'):
#             print(f"Skipping non-TIF file: {file_name}")
#             continue
        
#         print(f"Downloading: {file_name} (ID: {file_id})")
#         request = service.files().get_media(fileId=file_id)
#         fh = io.BytesIO()
#         downloader = MediaIoBaseDownload(fh, request)
#         done = False
        
#         while not done:
#             status, done = downloader.next_chunk()
#             if status:
#                 print(f"Download {int(status.progress() * 100)}% complete for {file_name}.")
        
#         destination_path = os.path.join(destination_dir, file_name)
#         with open(destination_path, 'wb') as f:
#             f.write(fh.getvalue())
#         print(f"Downloaded {file_name} to {destination_path}")

# def main():
#     try:
#         download_task()
#     except Exception as e:
#         print("Error during download task:", e)

# if __name__ == '__main__':
#     main()