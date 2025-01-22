import asyncio
import logging
import pandas as pd
import uuid

import os
import re

from core.sql_utils import get_connection
from fleet_info import read_fleet_info as fleet_info
from config import *

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

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

def standardize_model_type(model: str, type_value: str, make: str) -> tuple[str, str]:
    if not type_value or type_value =='x':
        return model.lower(), None

    model = model.lower()
    type_value = type_value.lower()
    make_lower = make.lower()
    
    for suffix in suffixes_to_remove:
        type_value = type_value.replace(f" {suffix}", "")
    
    if make_lower in mappings and model in mappings[make_lower]:
        model_info = mappings[make_lower][model]
        
        # Applique le nettoyage du modèle si spécifié
        if 'model_clean' in model_info:
            model = model_info['model_clean'](model)
            
        # Applique les patterns pour le type
        for pattern, replacement in model_info['patterns']:
            if re.search(pattern, type_value):
                return model.lower(), str(replacement).lower()

    return model.lower(), type_value.strip().lower()

async def process_vehicles(df: pd.DataFrame):
    """Traite les véhicules du DataFrame et les insère dans la base de données"""


    COUNTRY_MAPPING = {
        'NL': 'Netherlands',
    }

    with get_connection() as con:
        cursor = con.cursor()
        for index, vehicle in df.iterrows():
            try:
            # Gestion de la marque
                make_lower = vehicle['make'].lower()
                
                cursor.execute("""
                    SELECT id FROM make 
                    WHERE LOWER(make_name) = %s
                """, (make_lower,))
                
                make_result = cursor.fetchone()
                
                if not make_result:
                    logging.error(f"MAKE non trouvé pour la marque: {vehicle['make']} (mappé à {make_lower})")
                    continue
                make_id = make_result[0]
                # Standardisation du modèle et type
                model_name = vehicle['model'].strip() if pd.notna(vehicle['model']) else None
                type_value = vehicle['version'].strip() if pd.notna(vehicle['version']) else None
                if not model_name:
                    logging.error(f"Modèle manquant pour le véhicule VIN: {vehicle['vin']}")
                    continue

                model_name, type_value = standardize_model_type(model_name, type_value, vehicle['make'])

                # Recherche du modèle dans la base
                cursor.execute("""
                    SELECT id FROM vehicle_model 
                    WHERE LOWER(model_name) = %s 
                    AND (
                        (LOWER(type) = %s AND %s IS NOT NULL)
                        OR (type IS NULL AND %s IS NULL)
                    )
                    AND make_id = %s
                """, (model_name.lower(), type_value.lower() if type_value else None, 
                        type_value, type_value, make_id))
                
                result = cursor.fetchone()
                if result:
                    vehicle_model_id = result[0]
                else:
                    
                    vehicle_model_id = str(uuid.uuid4())
                    if type_value:
                        cursor.execute("""
                            INSERT INTO vehicle_model (id, model_name, type, make_id)
                            VALUES (%s, %s, %s, %s)
                            RETURNING id
                        """, (
                            vehicle_model_id,
                            model_name.lower(),
                            type_value.lower(),
                            make_id
                        ))
                    else:
                        cursor.execute("""
                            INSERT INTO vehicle_model (id, model_name, make_id)
                            VALUES (%s, %s, %s)
                            RETURNING id
                        """, (
                            vehicle_model_id,
                            model_name.lower(),
                            make_id
                        ))
                    vehicle_model_id = cursor.fetchone()[0]
                
                    logging.info(f"Créé nouveau modèle: {model_name} {type_value or ''} pour {vehicle['make']}") 
                # Récupération fleet_id
                cursor.execute("""
                    SELECT id FROM fleet 
                    WHERE LOWER(fleet_name) = LOWER(%s)
                """, (vehicle['owner'],))
                fleet_result = cursor.fetchone()
                if not fleet_result:
                    logging.error(f"Fleet non trouvée pour owner: {vehicle['owner']}")
                    continue
                fleet_id = fleet_result[0]
    
                # Gestion de la région
                if pd.isna(vehicle['country']):
                    logging.warning(f"Pays manquant pour le véhicule VIN: {vehicle['vin']}")
                    continue
                
                country = COUNTRY_MAPPING.get(vehicle['country'], vehicle['country'])
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
                # Gestion du véhicule
                cursor.execute("SELECT id FROM vehicle WHERE vin = %s", (vehicle['vin'],))
                vehicle_exists = cursor.fetchone()
                
                end_of_contract = convert_date_format(vehicle['end_of_contract'])
                start_date = convert_date_format(vehicle['start_date']) 
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
                        vehicle['licence_plate'],
                        end_of_contract,
                        start_date,
                        vehicle['vin']
                    ))
                    logging.info(f"Véhicule mis à jour avec VIN: {vehicle['vin']}")
                else:
                    vehicle_id = str(uuid.uuid4())
                    cursor.execute("""
                        INSERT INTO vehicle (
                            id, vin, fleet_id, region_id, vehicle_model_id,
                            licence_plate, end_of_contract_date, start_date
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        vehicle_id,
                        vehicle['vin'],
                        fleet_id,
                        region_id,
                        vehicle_model_id,
                        vehicle['licence_plate'],
                        end_of_contract,
                        start_date
                    ))
                    logging.info(f"Nouveau véhicule inséré avec VIN: {vehicle['vin']}")
                
            except Exception as e:
                logging.error(f"Erreur lors du traitement du véhicule {vehicle['VIN']}: {str(e)}")
                continue
        con.commit()

async def get_existing_model_metadata():
    """Récupère les métadonnées existantes des modèles de véhicules"""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                m.make_name,
                vm.model_name,
                vm.type,
                vm.url_image,
                vm.warranty_km,
                vm.warranty_date,
                vm.capacity
            FROM vehicle_model vm
            JOIN make m ON vm.make_id = o.id
            WHERE vm.url_image IS NOT NULL 
               OR vm.warranty_km IS NOT NULL 
               OR vm.warranty_date IS NOT NULL 
               OR vm.capacity IS NOT NULL
            ORDER BY m.make_name, vm.model_name, vm.type
        """)
        
        results = cursor.fetchall()
        
        if results:
            print("\nMétadonnées existantes des modèles :")
            print("--------------------------------------------------------------------------------")
            print("Marque | Modèle | Type | URL | Garantie km | Garantie années | Capacité")
            print("--------------------------------------------------------------------------------")
            for row in results:
                make, model, type_value, url, warranty_km, warranty_date, capacity = row
                print(f"{make} | {model} | {type_value or 'N/A'} | {url or 'N/A'} | {warranty_km or 'N/A'} | {warranty_date or 'N/A'} | {capacity or 'N/A'}")
            print("--------------------------------------------------------------------------------")
        else:
            print("Aucune métadonnée trouvée dans la base")
            
        return results
            
async def main(df: pd.DataFrame):
    try:
        logging.info(f"Nombre total de véhicules dans fleet_info: {len(df)}") #don't work at the moment
        df = df.query("make != 'tesla'")

        await process_vehicles(df)
        #metadata = await get_existing_model_metadata()
        
    except Exception as e:
        logging.error(f"Erreur dans le programme principal: {str(e)}")

if __name__ == "__main__":
    df = asyncio.run(fleet_info())
    asyncio.run(main(df))



