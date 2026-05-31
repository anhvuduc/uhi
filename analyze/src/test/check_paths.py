
import os
import glob

# Define base path relative to this script
# Assuming this script is in analyze/src/
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '../../data/raw')

CITIES = ['hanoi', 'danang', 'hcm', 'haiphong']

print(f"Checking data in: {DATA_DIR}")

if not os.path.exists(DATA_DIR):
    print(f"❌ Data directory not found: {DATA_DIR}")
else:
    print(f"✅ Data directory found: {DATA_DIR}")
    
    for city in CITIES:
        city_path = os.path.join(DATA_DIR, city)
        if not os.path.exists(city_path):
            print(f"  ❌ City directory not found: {city}")
        else:
            print(f"  ✅ City directory found: {city}")
            # Check for subdirectories (products)
            products = [d for d in os.listdir(city_path) if os.path.isdir(os.path.join(city_path, d))]
            if not products:
                 print(f"    ⚠️ No product directories found in {city}")
            else:
                for product in products:
                    product_path = os.path.join(city_path, product)
                    files = glob.glob(os.path.join(product_path, '*.tif'))
                    print(f"    - {product}: {len(files)} .tif files")

