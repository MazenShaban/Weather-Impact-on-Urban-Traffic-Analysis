import os
from hdfs import InsecureClient
import s3fs

# --- Configuration ---
MINIO_OPTS = {
    "key": "admin",
    "secret": "password123",
    "client_kwargs": {"endpoint_url": "http://localhost:9000"}
}
HDFS_URL = "http://localhost:9870" # WebHDFS Port
HDFS_USER = "root"                 # Default user for this docker image

def ingest_data():
    print("Starting Pipeline: MinIO (Silver) -> HDFS...")

    # 1. Connect to MinIO
    fs_minio = s3fs.S3FileSystem(**MINIO_OPTS)

    # 2. Connect to HDFS
    client_hdfs = InsecureClient(HDFS_URL, user=HDFS_USER)

    # 3. Create Required HDFS Folders [cite: 124-127]
    print("Creating HDFS directories...")
    client_hdfs.makedirs("/weather_data")
    client_hdfs.makedirs("/traffic_data")

    # 4. List Cleaned Files in Silver Bucket
    # Assuming your files are named 'weather_cleaned.parquet' and 'traffic_cleaned.parquet'
    silver_files = fs_minio.ls("silver")
    
    for file_path in silver_files:
        filename = os.path.basename(file_path)
        
        # Skip folders or non-parquet files
        if not filename.endswith(".parquet"):
            continue

        # Determine Destination Folder
        if "weather" in filename.lower():
            hdfs_dest = f"/weather_data/{filename}"
        elif "traffic" in filename.lower():
            hdfs_dest = f"/traffic_data/{filename}"
        else:
            hdfs_dest = f"/misc/{filename}"

        print(f"Transferring: {filename} -> {hdfs_dest}")

        # 5. Stream the Data (Read RAM -> Write RAM)
        # We open the MinIO file as a stream and pass it directly to HDFS write
        with fs_minio.open(file_path, 'rb') as source_stream:
            client_hdfs.write(hdfs_dest, source_stream, overwrite=True)

    print("✅ Data successfully ingested into HDFS.")

if __name__ == "__main__":
    ingest_data()