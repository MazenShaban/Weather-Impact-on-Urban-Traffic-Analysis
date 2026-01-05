import pandas as pd
import s3fs

# --- Configuration ---
MINIO_OPTS = {
    "key": "admin",
    "secret": "password123",
    "client_kwargs": {"endpoint_url": "http://localhost:9000"}
}

def merge_data():
    print("Phase 4: Merging Datasets (Silver -> Gold)...")
    
    # 1. Setup MinIO Connection
    # We pass the options directly to storage_options in pandas
    
    try:
        # 2. Read Cleaned Data from Silver Bucket
        print("Reading weather data...")
        weather_df = pd.read_parquet(
            "s3://silver/weather_cleaned.parquet", 
            storage_options=MINIO_OPTS
        )
        
        print("Reading traffic data...")
        traffic_df = pd.read_parquet(
            "s3://silver/traffic_cleaned.parquet", 
            storage_options=MINIO_OPTS
        )

        # 3. Pre-Merge Processing
        # Ensure date_time is datetime type (crucial for accurate merging)
        weather_df['date_time'] = pd.to_datetime(weather_df['date_time'])
        traffic_df['date_time'] = pd.to_datetime(traffic_df['date_time'])

        # Rename conflicting columns to avoid confusion
        # Both datasets have 'visibility_m', but they are slightly different (sensor noise)
        weather_df = weather_df.rename(columns={'visibility_m': 'visibility_weather'})
        traffic_df = traffic_df.rename(columns={'visibility_m': 'visibility_traffic'})

        # 4. Perform the Merge
        # We merge on 'date_time' and 'city' [cite: 134-136]
        print("Merging datasets...")
        merged_df = pd.merge(
            traffic_df, 
            weather_df, 
            on=['date_time', 'city'], 
            how='inner'
        )
        
        # 5. Validation
        print(f"Merged Data Shape: {merged_df.shape}")
        if len(merged_df) == 0:
            raise ValueError("Merge resulted in 0 rows! Check date_time formats.")
            
        # 6. Save to Gold Bucket 
        # This is your final analytical dataset
        output_path = "s3://gold/merged_data/merged_data.parquet"
        print(f"Saving to {output_path}...")
        
        merged_df.to_parquet(
            output_path, 
            storage_options=MINIO_OPTS, 
            index=False
        )
        
        print("✅ Merged dataset saved to Gold Layer.")
        
    except Exception as e:
        print(f"❌ Error during merge: {e}")

if __name__ == "__main__":
    merge_data()
    # Quick check
    df = pd.read_parquet("s3://gold/merged_data.parquet", storage_options=MINIO_OPTS)
    print(df.columns) 
    # Look for 'visibility_weather' AND 'visibility_traffic' to ensure rename worked