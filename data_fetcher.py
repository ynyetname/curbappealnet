import pandas as pd
import requests
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

api_key = "sk.eyJ1IjoieW55ZXRuYW1lIiwiYSI6ImNtamd4bHhreTFjYmozZ3Noem15Z2w0cDkifQ.murMJjGnbNZ1ANgoJ4JjdA"
train_image_dir = "C:\\dataset_curbappealnet\\satellite_ima\\train_images"
test_image_dir = "C:\\dataset_curbappealnet\\satellite_ima\\test_images"

train_curbappeal_csv = "C:\\dataset_curbappealnet\\train_curbappealnet.csv"
test_curbappeal_csv = "C:\\dataset_curbappealnet\\test_curbappealnet.csv"

zoom_level = 17
image_size = "700x700"

def fetch_image(lat, long, house_id, image_dir):  
    try:                                  
        lat = float(lat)
        long = float(long)
    except (ValueError, TypeError):  
        return f"Skipped {house_id}: Invalid coordinates"
    
    if not (-90 <= lat <= 90) or not (-180 <= long <= 180):
        return f"Skipped {house_id}: Out of range coordinates"
    
    filename = f"{image_dir}/{house_id}.jpg"
    
    if os.path.exists(filename):
        return f"Skipped {house_id}: Already exists"

    url = f"https://api.mapbox.com/styles/v1/mapbox/satellite-v9/static/{long},{lat},{zoom_level},0,0/{image_size}@2x?access_token={api_key}"

    try:
        response = requests.get(url, stream=True, timeout=10)
        if response.status_code == 200:
            with open(filename, 'wb') as f:
                for chunk in response.iter_content(1024):
                    f.write(chunk)
            return f"Downloaded: {house_id}"
        else:
            return f"Failed {house_id}: Status {response.status_code}"
    except Exception as e:
        return f"Error {house_id}: {e}"

def process_dataset(csv_path, image_dir, dataset_name, max_workers=20):
    if not os.path.exists(image_dir):
        os.makedirs(image_dir)

    if not os.path.exists(csv_path):
        print(f"Error: File not found at {csv_path}")
        return

    print(f"Loading {dataset_name} CSV dataset")
    df = pd.read_csv(csv_path)
    print(f"Columns found: {df.columns.tolist()}")
    print(f"Found {len(df)} properties. Starting parallel download with {max_workers} workers")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for _, row in df.iterrows():
            try:
                lat = row['lat']
                lon = row['long'] 
                house_id = row['id']
                futures.append(executor.submit(fetch_image, lat, lon, house_id, image_dir))
            except KeyError as e:
                print(f"Column error: {e}")
                break
        
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    print(result)
            except Exception as e:
                print(f"Error: {e}")

def main():
    
    process_dataset(train_curbappeal_csv, train_image_dir, "train", max_workers=20)
    
    process_dataset(test_curbappeal_csv, test_image_dir, "test", max_workers=20)

if __name__ == "__main__":
    main()
