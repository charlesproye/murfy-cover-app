from logging import getLogger
from typing import List, Dict
import pandas as pd
from sqlalchemy import create_engine, Engine, text
from sqlalchemy.exc import SQLAlchemyError
from core.sql_utils import get_sqlalchemy_engine, get_sqlalchemy_engine_prod
import numpy as np
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
def update_vehicle_models(engine: Engine, data: pd.DataFrame) -> None:
    """
    Update vehicle models in the database with new data using bulk updates.
    
    Args:
        engine: SQLAlchemy engine instance
        data: DataFrame containing vehicle model data to be updated
    """
    try:
        # Define columns that should be updated
        exclude_columns = {'id', 'created_at', 'oem_id', 'make_id', 'updated_at'}
        update_columns = [col for col in data.columns if col not in exclude_columns]
        
        # Construct the SET clause for the UPDATE statement
        update_sets = [f"{col} = :{col}" for col in update_columns]
        
        # Construct the UPDATE statement
        update_query = text(f"""
            UPDATE vehicle_model
            SET {', '.join(update_sets)}
            WHERE id = :id
        """)
        
        with engine.begin() as conn:
            updated_count = 0
            for _, row in data.iterrows():
                # Create parameters dictionary for the update
                params = {col: row[col] for col in update_columns}
                params['id'] = row['id']  # Add ID for WHERE clause
                
                # Execute the update
                result = conn.execute(update_query, params)
                updated_count += result.rowcount
            
            logging.info(f"Successfully updated {updated_count} vehicle models in dev database")
            
    except SQLAlchemyError as e:
        logging.error(f"Database error occurred: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error occurred: {str(e)}")
        raise

def get_injection_data() -> None:
    """
    Get injection data from production database.
    
    This function matches records based on version number between prod and dev databases.
    For matching records, it updates null values in development with production values,
    preserving existing non-null values in development.
    """
    
    try:
        prod_engine = get_sqlalchemy_engine_prod()
        dev_engine = get_sqlalchemy_engine()
        df_vehicle_model_prod = pd.read_sql("SELECT * FROM vehicle_model", prod_engine)
        df_vehicle_model_dev = pd.read_sql("SELECT * FROM vehicle_model", dev_engine)
        df_vehicle_model_dev.replace([np.nan, pd.NA], None, inplace=True)
        df_vehicle_model_prod.replace([np.nan, pd.NA], None, inplace=True)
        logging.info(f"Fetched {len(df_vehicle_model_prod)} records from prod and {len(df_vehicle_model_dev)} records from dev")
        
        match_version = pd.merge(
            df_vehicle_model_dev[df_vehicle_model_dev['version'].notna()],
            df_vehicle_model_prod[df_vehicle_model_prod['version'].notna()],
            on='version',
            how='left',
            suffixes=('', '_prod')
        )
        exclude_columns_version = {'oem_id', 'make_id', 'id', 'updated_at', 'created_at','version'}
        update_columns_version = list(set(df_vehicle_model_dev.columns) - exclude_columns_version)

        for col in update_columns_version:
            prod_col = f'{col}_prod'
            match_version[col] = match_version[prod_col]

        match_model_name_type = pd.merge(
            df_vehicle_model_dev[df_vehicle_model_dev['version'].isna()],
            df_vehicle_model_prod[df_vehicle_model_prod['version'].isna()],
            on=['model_name', 'type'],
            how='left',
            suffixes=('', '_prod')
        )
        exclude_columns_model_name_type = {'oem_id', 'make_id', 'id', 'updated_at', 'created_at','model_name', 'type'}
        update_columns_model_name_type = list(set(df_vehicle_model_dev.columns) - exclude_columns_model_name_type)

        for col in update_columns_model_name_type:
            prod_col = f'{col}_prod'
            match_model_name_type[col] = match_model_name_type[prod_col]

        merged_df = pd.concat([match_version, match_model_name_type])
        merged_df.replace([np.nan, pd.NA], None, inplace=True)
        merged_df = merged_df[['version', 'model_name', 'type', 'capacity', 'net_capacity', 'autonomy', 'warranty_km', 'warranty_date','id','oem_id','make_id','updated_at','created_at','battery_oem','battery_chemistry','battery_name','source']]

        logging.info(f"Preparing to inject {len(merged_df)} models from PROD to DEV")
        print(merged_df)
        return merged_df    
        
        
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        data_df = get_injection_data()
        dev_engine = get_sqlalchemy_engine()
        update_vehicle_models(dev_engine, data_df)
        logging.info("Database sync completed successfully")
    except Exception as e:
        logging.error(f"Script failed: {str(e)}")
        raise
