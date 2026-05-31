import os
import glob
from datetime import datetime

# Target directory
TARGET_DIR = '/Users/anh/Projects/wuhi/analyze/data/raw/haiphong/MXD13A1'

def parse_filename(filename):
    """
    Parses filename to extract start_date and end_date.
    Format: {City}_{Product}_{StartDate}_{EndDate}_mean.tif
    Example: haiphong_MXD13A1_20231101_20231130_mean.tif
    """
    try:
        parts = filename.replace('.tif', '').split('_')
        # Assuming standard format: city_product_start_end_mean
        # But sometimes city name might have underscores? 
        # Let's rely on the fact that dates are at index -3 and -2
        start_date_str = parts[-3]
        end_date_str = parts[-2]
        
        return start_date_str, end_date_str
    except Exception as e:
        print(f"⚠️ Cannot parse: {filename} ({e})")
        return None, None

def cleanup_files():
    if not os.path.exists(TARGET_DIR):
        print(f"❌ Directory not found: {TARGET_DIR}")
        return

    print(f"📂 Scanning: {TARGET_DIR}")
    files = glob.glob(os.path.join(TARGET_DIR, '*.tif'))
    
    # Group by Start Date
    groups = {}
    for file_path in files:
        filename = os.path.basename(file_path)
        start_date, end_date = parse_filename(filename)
        
        if start_date and end_date:
            if start_date not in groups:
                groups[start_date] = []
            groups[start_date].append({
                'path': file_path,
                'filename': filename,
                'end_date': end_date
            })

    # Process groups
    files_to_delete = []
    
    for s_date, items in groups.items():
        if len(items) > 1:
            # Sort by end_date (descending) -> Keep the latest one
            # String comparison for YYYYMMDD works fine
            items.sort(key=lambda x: x['end_date'], reverse=True)
            
            keep = items[0]
            remove_list = items[1:]
            
            print(f"\n🗓️  Start Date: {s_date}")
            print(f"   ✅ KEEP: {keep['filename']}")
            for item in remove_list:
                print(f"   ❌ DELETE: {item['filename']}")
                files_to_delete.append(item['path'])

    if not files_to_delete:
        print("\n✨ No redundant files found. Everything is clean!")
        return

    # Confirmation
    print(f"\n⚠️  Found {len(files_to_delete)} files to delete.")
    confirm = input("❓ Do you want to delete them? (y/n): ").strip().lower()
    
    if confirm == 'y':
        for file_path in files_to_delete:
            try:
                os.remove(file_path)
                print(f"🗑️  Deleted: {os.path.basename(file_path)}")
            except Exception as e:
                print(f"❌ Error deleting {os.path.basename(file_path)}: {e}")
        print("\n✅ Cleanup complete.")
    else:
        print("\n🚫 Operation cancelled.")

if __name__ == "__main__":
    cleanup_files()
