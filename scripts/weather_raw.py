import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

def generate_weather_dataset(
        n_rows = 5000,
        duplicate_ratio = 0.05,
        null_ratio = 0.1,
        outlier_ratio = 0.1,
        bad_format_ratio = 0.01
    ):
    np.random.seed(42)  # Controls randomness from NumPy, used in: np.random.uniform(), np.random.randint()
    random.seed(42) # Controls randomness from Python's built-in random 'library', used in: random.random(), random.choice()

    # Generate base valid timestamps
    base = datetime(2024,1,1)
    
    # ---- Helper functions ----
    def generate_datetime(index):
        # Inject total garbage occasionally (1% chance)
        if random.random() < bad_format_ratio:
            return random.choice(["2099-13-40 25:61", "Unknown", "TBD"])
        
        # Sequential timestamps: every 2 hours
        dt = base + timedelta(hours=2 * index)

        # Format variations (still valid dates)
        return random.choice([
            dt.strftime("%Y-%m-%d %H:%M"),
            dt.strftime("%d/%m/%Y %I%p"),
            dt.strftime("%Y-%m-%dT%H:%MZ")
        ])


    def get_season(dt_str):
        try:
            dt = pd.to_datetime(dt_str, errors="coerce")
            if pd.isna(dt):
                return random.choice(["Winter","Spring","Summer","Autumn"])
            
            m = dt.month
            if m in [12,1,2]: return "Winter"
            elif m in [3,4,5]: return "Spring"
            elif m in [6,7,8]: return "Summer"
            return "Autumn"
        
        except:
            return None


    def temperature_by_season(season):
        ranges = {
            "Winter": (-5,15),
            "Spring": (5,20),
            "Summer": (10,35),
            "Autumn": (5,25)
        }
        low, high = ranges.get(season, (0,30))  # (0,30) is fallback
        return round(random.uniform(low, high), 2)


    def generate_humidity(season):
        # Season-based realistic ranges
        season_ranges = {
            "Winter": (40, 90),
            "Spring": (30, 80),
            "Summer": (20, 70),
            "Autumn": (50, 100)
        }
        low, high = season_ranges.get(season, (20, 100)) # (20, 100) is fallback
        return random.randint(low, high)


    def generate_rain(humidity):
        if humidity is None:
            return round(random.uniform(0, 20), 2)
        
        # Low humidity usually means NO rain
        if humidity < 60:
            return 0.0  
            
        # Even with high humidity, it doesn't ALWAYS rain. 
        # Let's say it rains 30% of the time when humidity is high.
        if random.random() > 0.3: 
            return 0.0

        # If it DOES rain:
        if humidity < 80:
            return round(random.uniform(1, 15), 2) # Light rain
        else:
            return round(random.uniform(10, 80), 2) # Heavy rain


    def generate_weather_condition(rain_mm, temperature_c):
        # Snow chance if temperature is cold enough
        if temperature_c is not None and temperature_c <= 5:
            if random.random() < 0.4:
                return "Snow"

        # Rain-based conditions
        if rain_mm is None:
            return random.choice(["Clear", "Fog"])

        if rain_mm >= 50:
            return "Storm" if random.random() < 0.6 else "Rain"
        elif rain_mm >= 25:
            return "Rain"
        elif rain_mm >= 10:
            return "Rain" if random.random() < 0.6 else "Clear"
        elif rain_mm > 0:
            return random.choice(["Clear", "Fog"])
        else:
            return "Clear"

    
    def generate_wind_speed(weather_condition):
        # Storm → stronger winds more likely
        if weather_condition == "Storm":
            if random.random() < 0.2:                 # extreme outliers
                return round(random.uniform(100, 150), 2)
            elif random.random() < 0.6:               # strong typical storms
                return round(random.uniform(50, 80), 2)

        return round(random.uniform(0, 80), 2) # typical range


    def generate_visibility(weather_condition):
        # Small chance of garbage string values
        if random.random() < 0.03:   # ~3% messy textual data
            return random.choice(["unknown", "N/A", "error", "???"])

        # Foggy/rainy → low visibility likely
        if weather_condition in ["Fog", "Rain", "Storm", "Snow"]:
            if random.random() < 0.15:
                return random.randint(50, 1000) # extreme low visibility
            elif random.random() < 0.6:
                return random.randint(1000, 8000)   # poor to moderate visibility

        return random.randint(8000, 12000)  # good visibility


    def generate_air_pressure(weather_condition, temperature_c):
        # Base pressure range
        low, high = 990, 1030

        # Weather condition influence (strongest)
        if weather_condition == "Storm":
            low = random.uniform(950, 970)
            high = random.uniform(980, 1000)

        elif weather_condition in ["Rain", "Snow"]:
            low = random.uniform(960, 990)
            high = random.uniform(1000, 1050)

        # Temperature influence (secondary)
        if temperature_c is not None:
            if temperature_c > 30:
                low -= 10
                high -= 10
            elif temperature_c < 0:
                low += 10
                high += 10

        # Normal pressure simulation
        return round(random.uniform(low, high), 2)


    def maybe_null(value):
        return None if random.random() < null_ratio else value


    def with_outliers(normal_value, outlier_low, outlier_high):
        if random.random() < outlier_ratio:
            if random.random() < 0.5:
                return random.uniform(outlier_low - 25, outlier_low)
            return random.uniform(outlier_high, outlier_high + 25)
        return normal_value

    # ---- Data generation ----
    rows = []

    for i in range(n_rows):
        date_time = maybe_null(generate_datetime(i))
        season = maybe_null(get_season(date_time))
        temperature_c = maybe_null(with_outliers(temperature_by_season(season), -30, 60))
        humidity = maybe_null(with_outliers(generate_humidity(season), -10, 150))
        rain_mm = maybe_null(with_outliers(generate_rain(humidity), 100, 200))
        weather_condition = maybe_null(generate_weather_condition(rain_mm, temperature_c))
        wind_speed_kmh = maybe_null(with_outliers(generate_wind_speed(weather_condition), 200, 350))
        visibility_m = maybe_null(with_outliers(generate_visibility(weather_condition), 50000, 120000))
        air_pressure_hpa = maybe_null(with_outliers(generate_air_pressure(weather_condition, temperature_c), 900, 1100))

        row = {
            "weather_id": maybe_null(5001 + i),
            "date_time": date_time,
            "city": maybe_null("London"),
            "season": season,
            "temperature_c": temperature_c,
            "humidity": humidity,
            "rain_mm": rain_mm,
            "weather_condition": weather_condition,
            "wind_speed_kmh": wind_speed_kmh,
            "visibility_m": visibility_m,
            "air_pressure_hpa": air_pressure_hpa
        }

        rows.append(row)

    df = pd.DataFrame(rows)

    # Add duplicates
    dup_count = int(n_rows * duplicate_ratio)
    df = pd.concat([df, df.sample(dup_count)], ignore_index=True)

    return df


if __name__ == "__main__":
    print("Generating weather dataset...")
    
    # 1. Call the function to get the DataFrame
    weather_df = generate_weather_dataset()

    # 2. Define the filename
    csv_filename = "weather_raw.csv"

    # 3. Save to CSV
    # index=False is crucial: it prevents pandas from saving the row numbers as a separate column
    weather_df.to_csv(f"../data/raw/{csv_filename}", index=False)
    
    print(f"Success! Generated {len(weather_df)} rows.")
    print(f"File saved locally as: {csv_filename}")
