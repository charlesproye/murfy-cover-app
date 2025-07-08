import pandas as pd
from core.sql_utils import get_connection

def get_odometer_delta():    
    try:
        # Connect to the database using existing environment variables
        with get_connection() as con:
            cursor = con.cursor()
            
            # Execute query and load results into a pandas DataFrame
            cursor.execute("""
                SELECT v.vin, MAX(vd.odometer) - MIN(vd.odometer) AS odometer_delta
                FROM vehicle_data vd
                JOIN vehicle v ON v.id = vd.vehicle_id
                GROUP BY v.vin;""")
            
            # Convert cursor results to DataFrame with column names
            columns = ['vin', 'odometer_delta']
            df = pd.DataFrame(cursor.fetchall(), columns=columns)
            
            # Format the odometer_delta column to 2 decimal places
            df['odometer_delta'] = df['odometer_delta'].round(2)
            
            # Print results
            print("\nOdometer Delta by VIN:")
            print(df.to_string(index=False))
            
            # Print summary statistics
            print("\nSummary Statistics:")
            print(f"Total number of vehicles: {len(df)}")
            print(f"Average odometer delta: {df['odometer_delta'].mean():.2f}")
            print(f"Maximum odometer delta: {df['odometer_delta'].max():.2f}")
            print(f"Minimum odometer delta: {df['odometer_delta'].min():.2f}")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    get_odometer_delta()

