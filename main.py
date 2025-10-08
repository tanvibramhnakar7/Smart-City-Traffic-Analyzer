import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pymysql

# --- Phase 1: Extraction (E) - Sourcing and Reading Data ---

def generate_synthetic_traffic_data(num_rows=10000, filename='traffic_data.csv'):
    """
    Generates a synthetic traffic dataset and saves it to a CSV file.
    The data simulates realistic traffic patterns over a 30-day period.
    """
    print("Generating synthetic traffic data...")
    
    # Define start and end dates for data simulation
    start_date = datetime(2023, 10, 1)
    end_date = start_date + timedelta(days=30)
    
    # Generate timestamps for each row at 15-minute intervals
    timestamps = [start_date + timedelta(minutes=15 * i) for i in range(num_rows)]

    # Generate location IDs (e.g., 10 distinct locations)
    location_ids = np.random.choice(range(1, 11), num_rows)

    # Simulate vehicle count with peaks during morning (7-9 AM) and evening (4-6 PM)
    hours = [ts.hour for ts in timestamps]
    vehicle_counts = []
    for h in hours:
        # Base count for a given hour
        base_count = np.random.randint(50, 200)
        # Apply a multiplier for peak hours
        if 7 <= h <= 9 or 16 <= h <= 18:
            vehicle_counts.append(int(base_count * np.random.uniform(1.5, 2.5)))
        else:
            vehicle_counts.append(base_count)

    # Simulate average speed, inversely related to vehicle count
    avg_speeds = []
    for count in vehicle_counts:
        # Base speed
        base_speed = np.random.uniform(30, 80)
        # Decrease speed for high traffic
        if count > 200:
            avg_speeds.append(max(5, base_speed - np.random.uniform(10, 40)))
        else:
            avg_speeds.append(base_speed)

    # Create a pandas DataFrame
    data = {
        'timestamp': timestamps,
        'location_id': location_ids,
        'vehicle_count': vehicle_counts,
        'average_speed': avg_speeds
    }
    df = pd.DataFrame(data)

    # Introduce some missing values to test the cleaning logic
    df.loc[df.sample(frac=0.01).index, 'vehicle_count'] = np.nan
    df.loc[df.sample(frac=0.01).index, 'average_speed'] = np.nan

    # Save the DataFrame to a CSV file
    df.to_csv(filename, index=False)
    print(f"Synthetic data saved to {filename}")

    return pd.read_csv(filename)


# --- Phase 2: Transformation (T) - Cleaning and Preparing the Data ---

def transform_traffic_data(df):
    """
    Cleans, validates, and transforms the raw traffic data DataFrame.
    """
    print("Starting data transformation...")

    # Step 1: Data Cleaning
    # Convert 'timestamp' to datetime objects
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Handle missing values using backward fill and then forward fill
    # This interpolates values based on adjacent valid entries
    df['vehicle_count'] = df['vehicle_count'].bfill().ffill()
    df['average_speed'] = df['average_speed'].bfill().ffill()

    # Validate and clean unrealistic values (e.g., speed > 200 km/h)
    # Capping speeds at a maximum of 120 km/h (a reasonable highway limit)
    df['average_speed'] = df['average_speed'].apply(lambda x: min(x, 120))

    # Step 2: Feature Engineering
    # Create new features from the timestamp
    df['hour_of_day'] = df['timestamp'].dt.hour
    df['day_of_week'] = df['timestamp'].dt.day_name()
    df['is_weekend'] = df['timestamp'].dt.dayofweek >= 5

    # Step 3: Categorize traffic congestion
    # Define a function to categorize based on speed and count
    def categorize_traffic(row):
        speed = row['average_speed']
        count = row['vehicle_count']
        if count > 300 and speed < 20:
            return 'congested'
        elif count > 150 and speed < 40:
            return 'moderate'
        else:
            return 'low'

    df['traffic_category'] = df.apply(categorize_traffic, axis=1)

    print("Data transformation complete.")
    return df


# --- Phase 3: Loading (L) - Storing Data in MySQL ---

def load_data_to_mysql(df, db_config):
    """
    Loads the transformed DataFrame into a MySQL database.
    """
    print("Connecting to MySQL database...")
    
    # MySQL connection string
    # Format: mysql+pymysql://user:password@host/dbname
    connection_string = (
        f"mysql+pymysql://{db_config['user']}:{db_config['password']}@"
        f"{db_config['host']}/{db_config['database']}"
    )

    try:
        engine = create_engine(connection_string)
        
        # Create a simple 'locations' dimension table
        locations_df = pd.DataFrame({
            'location_id': range(1, 11),
            'street_name': [f'Street {i}' for i in range(1, 11)],
            'area': [f'Area {np.random.choice(["North", "South", "East", "West"])}' for _ in range(10)]
        })
        
        # Load the locations DataFrame into the 'locations' table
        print("Loading locations data into 'locations' table...")
        locations_df.to_sql('locations', con=engine, if_exists='replace', index=False)

        # Load the main traffic data into the 'traffic_data' table
        print("Loading transformed traffic data into 'traffic_data' table...")
        df.to_sql('traffic_data', con=engine, if_exists='replace', index=False)

        print("Data successfully loaded into MySQL.")

    except Exception as e:
        print(f"An error occurred: {e}")
        print("Please ensure your MySQL server is running and the credentials are correct.")
        print("Also, make sure you have the 'pymysql' and 'cryptography' libraries installed.")


def main():
    """
    Main function to run the entire ETL pipeline.
    """
    # Define your MySQL database configuration here
    # Important: Replace with your actual credentials
    mysql_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'durgesh',
        'database': 'traffic_db'
    }

    # Phase 1: Extraction
    raw_df = generate_synthetic_traffic_data()
    print("\nSample of raw data:")
    print(raw_df.head())

    # Phase 2: Transformation
    transformed_df = transform_traffic_data(raw_df)
    print("\nSample of transformed data with new features:")
    print(transformed_df.head())

    # Phase 3: Loading
    load_data_to_mysql(transformed_df, mysql_config)


if __name__ == "__main__":
    main()
