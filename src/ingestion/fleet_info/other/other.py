import asyncio
import logging
import pandas as pd
import uuid
from core.sql_utils import get_connection
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def read_fleet_info(ownership_filter: str = None) -> pd.DataFrame:
    """
    Lit le fichier fleet_info.csv et retourne un DataFrame
    
    Args:
        ownership_filter (str, optional): Filtre pour ne garder qu'un certain type d'ownership
    """
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(current_dir, '..', 'data', 'fleet_info.csv')
        
        df = pd.read_csv(file_path)
        
        df.columns = [col if col != " " else "brand" for col in df.columns]
        
        # Exclure Tesla car géré en solo
        df = df[df['brand'].str.lower() != 'tesla']
        
        if ownership_filter:
            df['Ownership_lower'] = df['Ownership '].str.lower()
            df = df[df['Ownership_lower'] == ownership_filter.lower()]
            df = df.drop('Ownership_lower', axis=1)
            
            logging.info(f"Filtré pour Ownership = {ownership_filter}")
            logging.info(f"Nombre de véhicules après filtrage: {len(df)}")
        
        
        return df
        
    except FileNotFoundError:
        logging.error(f"Le fichier fleet_info.csv n'a pas été trouvé dans {file_path}")
        raise
    except Exception as e:
        logging.error(f"Erreur lors de la lecture du fichier CSV: {str(e)}")
        raise

def convert_date_format(date_str):
    """Convertit les différents formats de date en format YYYY-MM-DD"""
    if pd.isna(date_str):
        return None
    try:
        date_str = date_str.split()[0]
        
        if '.' in date_str:  # Format DD.MM.YYYY
            day, month, year = date_str.split('.')
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        elif '/' in date_str:  # Format MM/DD/YYYY
            month, day, year = date_str.split('/')
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        elif '-' in date_str:  # Format DD-MM-YYYY
            day, month, year = date_str.split('-')
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        else:
            logging.warning(f"Format de date non reconnu: {date_str}")
            return None
    except Exception as e:
        logging.warning(f"Format de date invalide: {date_str}, erreur: {str(e)}")
        return None

async def process_vehicles(df: pd.DataFrame):
    """Traite les véhicules du DataFrame et les insère dans la base de données"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    models_file = os.path.join(current_dir, '..', 'data', 'models_info.csv')
    
    BRAND_MAPPING = {
        'volvo': 'volvo cars'
    }

    COUNTRY_MAPPING = {
        'NL': 'Netherlands',
    }
    try:
        models_df = pd.read_csv(models_file)
        models_df['manufacturer'] = models_df['manufacturer'].str.lower()
        models_df['model'] = models_df['model'].str.lower()
        models_df['type'] = models_df['type'].str.lower()
    except Exception as e:
        logging.error(f"Erreur lors de la lecture du fichier models_info.csv: {str(e)}")
        raise

    with get_connection() as conn:
        cursor = conn.cursor()
        
        for index, vehicle in df.iterrows():
            try:
                brand_lower = vehicle['brand'].lower()
                brand_lower = BRAND_MAPPING.get(brand_lower, brand_lower) 
                
                cursor.execute("""
                    SELECT id FROM oem 
                    WHERE LOWER(oem_name) = %s
                """, (brand_lower,))
                
                oem_result = cursor.fetchone()
                if not oem_result:
                    logging.error(f"OEM non trouvé pour la marque: {vehicle['brand']} (mappé à {brand_lower})")
                    continue
                oem_id = oem_result[0]
                
                model_parts = vehicle['Model'].lower().split()
                found_model = False
                
                for _, ref_model in models_df[models_df['manufacturer'] == brand_lower].iterrows():
                    if ref_model['model'] in ' '.join(model_parts):
                        model_name = ref_model['model']
                        remaining_text = ' '.join(model_parts).replace(model_name, '').strip()
                        model_type = ref_model['type'] if pd.notna(ref_model['type']) else remaining_text
                        found_model = True
                        break
                
                if not found_model:
                    model_name = vehicle['Model'].lower()
                    model_type = None
                
                if model_type:
                    cursor.execute("""
                        SELECT id FROM vehicle_model 
                        WHERE LOWER(model_name) = %s 
                        AND LOWER(type) = %s 
                        AND oem_id = %s
                    """, (model_name, model_type, oem_id))
                else:
                    cursor.execute("""
                        SELECT id FROM vehicle_model 
                        WHERE LOWER(model_name) = %s 
                        AND type IS NULL 
                        AND oem_id = %s
                    """, (model_name, oem_id))
                
                result = cursor.fetchone()
                if result:
                    vehicle_model_id = result[0]
                else:
                    vehicle_model_id = str(uuid.uuid4())
                    if model_type:
                        cursor.execute("""
                            INSERT INTO vehicle_model (id, model_name, type, oem_id)
                            VALUES (%s, %s, %s, %s)
                            RETURNING id
                        """, (
                            vehicle_model_id,
                            model_name,
                            model_type,
                            oem_id
                        ))
                    else:
                        cursor.execute("""
                            INSERT INTO vehicle_model (id, model_name, oem_id)
                            VALUES (%s, %s, %s)
                            RETURNING id
                        """, (
                            vehicle_model_id,
                            model_name,
                            oem_id
                        ))
                    vehicle_model_id = cursor.fetchone()[0]
                    logging.info(f"Créé nouveau modèle: {model_name} {model_type or ''} pour {vehicle['brand']}")

                # 3. Récupérer fleet_id
                cursor.execute("""
                    SELECT id FROM fleet 
                    WHERE LOWER(fleet_name) = LOWER(%s)
                """, (vehicle['Ownership '],))
                
                fleet_result = cursor.fetchone()
                if not fleet_result:
                    logging.error(f"Fleet non trouvée pour ownership: {vehicle['Ownership ']}")
                    continue
                fleet_id = fleet_result[0]

                if pd.isna(vehicle['Country']):
                    logging.warning(f"Pays manquant pour le véhicule VIN: {vehicle['VIN']}")
                    continue
                
                country = COUNTRY_MAPPING.get(vehicle['Country'], vehicle['Country'])
                cursor.execute("""
                    SELECT id FROM region 
                    WHERE LOWER(region_name) = LOWER(%s)
                """, (country,))
                
                region_result = cursor.fetchone()
                if not region_result:
                    region_id = str(uuid.uuid4())
                    cursor.execute("""
                        INSERT INTO region (id, region_name)
                        VALUES (%s, %s)
                        RETURNING id
                    """, (region_id, country))
                    region_id = cursor.fetchone()[0]
                    logging.info(f"Nouvelle région créée: {country}")
                else:
                    region_id = region_result[0]
                
                cursor.execute("SELECT id FROM vehicle WHERE vin = %s", (vehicle['VIN'],))
                vehicle_exists = cursor.fetchone()
                
                end_of_contract = convert_date_format(vehicle['End of Contract'])
                start_date = convert_date_format(vehicle['Start Date'])
                
                if vehicle_exists:
                    cursor.execute("""
                        UPDATE vehicle 
                        SET fleet_id = %s,
                            region_id = %s,
                            vehicle_model_id = %s,
                            licence_plate = %s,
                            end_of_contract_date = %s,
                            start_date = %s
                        WHERE vin = %s
                    """, (
                        fleet_id,
                        region_id,
                        vehicle_model_id,
                        vehicle['Licence plate'],
                        end_of_contract,
                        start_date,
                        vehicle['VIN']
                    ))
                    logging.info(f"Véhicule mis à jour avec VIN: {vehicle['VIN']}")
                else:
                    vehicle_id = str(uuid.uuid4())
                    cursor.execute("""
                        INSERT INTO vehicle (
                            id, vin, fleet_id, region_id, vehicle_model_id,
                            licence_plate, end_of_contract_date, start_date
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        vehicle_id,
                        vehicle['VIN'],
                        fleet_id,
                        region_id,
                        vehicle_model_id,
                        vehicle['Licence plate'],
                        end_of_contract,
                        start_date
                    ))
                    logging.info(f"Nouveau véhicule inséré avec VIN: {vehicle['VIN']}")
                
            except Exception as e:
                logging.error(f"Erreur lors du traitement du véhicule {vehicle['VIN']}: {str(e)}")
                continue
        
        conn.commit()

async def main():
    try:
        #on peut mettre ce qu'on veut ici
        ownership_filter = "Ayvens" 
        df = await read_fleet_info(ownership_filter)
        logging.info(f"Nombre total de véhicules dans fleet_info: {len(df)}")
        
        await process_vehicles(df)
        
    except Exception as e:
        logging.error(f"Erreur dans le programme principal: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())

