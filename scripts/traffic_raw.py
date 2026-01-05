import pandas as pd
import numpy as np
import random
# from datetime import datetime, timedelta
import os
# import weather_raw

def generate_traffic_dataset(
        weather_df,  # We pass the whole weather dataframe here, it's safer!
        n_rows = 5000,
        duplicate_ratio = 0.05,
        null_ratio = 0.1,
        outlier_ratio = 0.1
        # bad_format_ratio = 0.01
    ):
    # Re-seeding ensures this script's randomness is reproducible
    np.random.seed(43) 
    random.seed(43)

    # # Generate base valid timestamps
    # base = datetime(2024,1,1)
    
    # # ---- Helper functions ----
    def generate_area():
        return random.choice(['Camden', 'Chelsea', 'Islington', 'Southwark', 'Kensington', 'Westminster', 'Greenwich'])


    def generate_vehicle_count(time_str):
        try:
            dt = pd.to_datetime(time_str, errors='coerce')
            if pd.isna(dt): return random.randint(100, 3000)
            
            hour = dt.hour
            # Rush hours: 7-9 AM and 4-7 PM
            if (7 <= hour <= 9) or (16 <= hour <= 19):
                return random.randint(2000, 5000)
            elif 0 <= hour <= 5: # Late night
                return random.randint(0, 500)
            else:
                return random.randint(800, 2500)
        except:
            return random.randint(500, 3000)
        

    def generate_road_condition(weather_condition):
        if weather_condition == 'Snow': return 'Snowy'
        elif weather_condition in ['Rain', 'Storm']: return 'Wet'
        
        if random.random() < 0.05: return 'Damaged'
        return 'Dry'
    

    def calculate_avg_speed(vehicle_count, weather_condition, road_condition):
        # Default base speed
        base_speed = 60
        try:
            # Traffic Density Impact
            if vehicle_count > 3000: 
                base_speed = round(random.uniform(10, 30), 2) # High Traffic = Slow
            elif vehicle_count > 1500: 
                base_speed = round(random.uniform(30, 50), 2)
            else:
                base_speed = round(random.uniform(50, 90), 2)
        except:
            base_speed = round(random.uniform(60, 90), 2)

        # Weather Impact
        if weather_condition in ['Rain', 'Snow', 'Storm']:
            base_speed -= 15
        elif weather_condition == 'Fog':
            base_speed -= 10
            
        # Road Condition Impact
        if road_condition == 'Damaged':
            base_speed -= 20
            
        # Add randomness
        final_speed = base_speed + random.uniform(-10, 10)
        return max(3.0, round(final_speed, 2))


    def determine_congestion(vehicle_count, avg_speed):
        try:
            if vehicle_count > 3500 or avg_speed < 15: return "High"
            elif vehicle_count > 1500 or avg_speed < 35: return "Medium"
            else: return "Low"
        except: return "Low"


    def generate_accidents(weather_condition, congestion_level):
        prob = 0.05
        if weather_condition in ['Rain', 'Storm', 'Snow', 'Fog']: prob += 0.15
        if congestion_level == 'High': prob += 0.10
            
        if random.random() < prob:
            return random.choice([1, 1, 1, 2, 2, 3])
        return 0


    # LOGIC: Don't re-generate visibility. Noise are added as it may differ from weather station sensor.
    def get_traffic_visibility(weather_vis_value):
        try:
            base_vis = weather_vis_value
            # Add -500m to +500m noise, but don't go below 0
            noise = random.randint(-500, 500)
            return max(0, int(base_vis + noise))
        except:
            return 10000 # Default if weather data is messy/text


    def maybe_null(value):
        return None if random.random() < null_ratio else value
    

    def with_outliers(normal_value, outlier_low, outlier_high):
        if random.random() < outlier_ratio:
            if random.random() < 0.5:
                return random.uniform(outlier_low - 25, outlier_low)
            return random.uniform(outlier_high, outlier_high + 25)
        return normal_value

    # ---- Data Generation Loop ----
    rows = []

    # Iterate using index to match rows ensuring 1-to-1 mapping
    limit = len(weather_df)

    # Iterate through weather_df to ensure 1-to-1 mapping
    for i in range(limit):
        # Get data from wheather raw (Using .iloc for safety)
        weather_row = weather_df.iloc[i]

        # CRITICAL: Use the DATE from weather, or they won't match in Phase 4
        date_time = weather_row['date_time']
        area = maybe_null(generate_area())
        vehicle_count = maybe_null(with_outliers(generate_vehicle_count(date_time),20000, 30000))
        road_condition = maybe_null(generate_road_condition(weather_row['weather_condition']))
        avg_speed_kmh = maybe_null(with_outliers(calculate_avg_speed(vehicle_count, weather_row['weather_condition'], road_condition), -1, 500))
        congestion_level = maybe_null(determine_congestion(vehicle_count, avg_speed_kmh))
        accident_count = maybe_null(with_outliers(generate_accidents(weather_row['weather_condition'], congestion_level), 20, 50))
        visibility_m = maybe_null(with_outliers(get_traffic_visibility(weather_row['visibility_m']), 50000, 120000))

        row = {
            "traffic_id": maybe_null(9001 + i),
            "date_time": date_time, 
            "city": maybe_null("London"),
            "area": area,
            "vehicle_count": vehicle_count,
            "road_condition": road_condition,
            "avg_speed_kmh": avg_speed_kmh,
            "congestion_level": congestion_level,
            "accident_count": accident_count,
            "visibility_m": visibility_m
        }
        
        rows.append(row)

    df = pd.DataFrame(rows)

    # Add duplicates
    dup_count = int(n_rows * duplicate_ratio)
    df = pd.concat([df, df.sample(dup_count)], ignore_index=True)

    return df

# ==========================================
# EXECUTION BLOCK
# ==========================================
if __name__ == "__main__":
    print("Generating traffic dataset...")

    # 1. LOAD the existing weather file (Best Practice!)
    # If weather_raw.py was modified later (e.g., temperature range was changed) but traffic_raw.py was forgotten to be updated, the Traffic generation will be based on new weather logic, but the saved weather.csv will contain old weather logic. The data will be out of sync.

    # Check if file exists first
    weather_file = "../data/raw/weather_raw.csv"
    
    if os.path.exists(weather_file):
        print(f"Loading existing {weather_file}...")
        weather_df = pd.read_csv(weather_file)
    else:
        # # Fallback: Generate it if missing (requires importing the module)
        # print(f"File {weather_file} not found. Generating fresh...")
        # weather_df = weather_raw.generate_weather_dataset()
        # weather_df.to_csv(weather_file, index=False)
        print(f"File {weather_file} not found. Please run weather_raw.py first.")
        exit() # Stop if no weather file found

    # Generate Traffic
    traffic_df = generate_traffic_dataset(weather_df)
    
    # 2. Define the filename
    csv_filename = "traffic_raw.csv"

    # 3. Save to CSV
    # index=False is crucial: it prevents pandas from saving the row numbers as a separate column
    traffic_df.to_csv(f"../data/raw/{csv_filename}", index=False)
    
    print(f"Success! Generated {len(traffic_df)} rows. Traffic Data Created (with dependencies on Weather).")
    print(f"File saved locally as: {csv_filename}")
