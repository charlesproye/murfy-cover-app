'''
import pandas as pd
import asyncio
import logging
import os
from tesla import main as tesla_main
from other import main as other_main
from core.sql_utils import get_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def get_existing_vehicles_info():
    """Récupère les informations détaillées des véhicules existants en base de données"""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                v.vin,
                m.make_name,
                vm.model_name,
                vm.type as version,
                v.activation_status,
                v.start_date,
                v.end_of_contract_date
            FROM vehicle v
            JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
            JOIN make m ON vm.make_id = m.id
        """)
        return {row[0]: {
            'make': row[1].lower(),
            'model': row[2].lower(),
            'version': row[3].lower() if row[3] else None,
            'activation': row[4],
            'start_date': row[5],
            'end_of_contract': row[6]
        } for row in cursor.fetchall()}

async def process_vehicles():
    try:
        # Obtenir le chemin absolu du répertoire courant
        current_dir = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(current_dir, 'data', 'final_poc.csv')
        
        # Lire le CSV
        df = pd.read_csv(csv_path)
        logging.info(f"Nombre total de véhicules dans le CSV: {len(df)}")

        # Nettoyer les noms de colonnes
        df.columns = df.columns.str.strip()
        
        # Afficher les colonnes pour le debug
        logging.info(f"Colonnes du CSV: {df.columns.tolist()}")
        
        # Nettoyer les données du CSV
        df['VIN'] = df['VIN'].str.strip()
        df['Make'] = df['Make'].str.strip().str.lower()
        df['Model'] = df['Model'].str.strip().str.lower()
        df['Type'] = df['Type'].str.strip().str.lower()

        # Récupérer les informations des véhicules existants
        existing_vehicles = await get_existing_vehicles_info()
        logging.info(f"Nombre de véhicules existants en base: {len(existing_vehicles)}")

        # Vérifier les différences
        missing_vins = set()
        different_info_vins = set()

        for _, row in df.iterrows():
            vin = row['VIN']
            if vin not in existing_vehicles:
                missing_vins.add(vin)
            else:
                # Comparer les informations
                db_info = existing_vehicles[vin]
                if (db_info['make'] != row['Make'].lower() or
                    db_info['model'] != row['Model'].lower() or
                    (db_info['version'] or '').lower() != (row['Type'] or '').lower()):
                    different_info_vins.add(vin)
                    logging.info(f"\nDifférences trouvées pour le VIN {vin}:")
                    logging.info(f"CSV  -> Make: {row['Make']}, Model: {row['Model']}, Type: {row['Type']}")
                    logging.info(f"DB   -> Make: {db_info['make']}, Model: {db_info['model']}, Type: {db_info['version']}")

        if missing_vins:
            logging.info(f"\nVINs présents dans le CSV mais manquants en production ({len(missing_vins)}):")
            for vin in missing_vins:
                logging.info(f"- {vin}")

            # Préparer les données pour l'insertion
            df_to_insert = df[df['VIN'].isin(missing_vins)].copy()
            
            # Préparer les données pour le traitement
            df_to_insert = df_to_insert.rename(columns={
                'VIN': 'vin',
                'Make': 'make',
                'Model': 'model',
                'Type': 'version',
                'Start Date': 'start_date',
                'Still monitored': 'activation'
            })

            # Convertir les valeurs booléennes numpy en bool Python standard
            df_to_insert['activation'] = df_to_insert['activation'].astype(bool).tolist()

            # Ajouter les colonnes nécessaires
            df_to_insert['oem'] = df_to_insert['make'].str.upper()
            df_to_insert['owner'] = 'Ayvens'  # Par défaut
            df_to_insert['country'] = 'Netherlands'  # Par défaut
            df_to_insert['licence_plate'] = ''  # Vide par défaut
            df_to_insert['end_of_contract'] = df_to_insert['Last Date']  # Utiliser Last Date comme end_of_contract

            # Séparer Tesla des autres
            tesla_df = df_to_insert[df_to_insert['make'] == 'tesla'].copy()
            other_df = df_to_insert[df_to_insert['make'] != 'tesla'].copy()

            # Log des VINs par marque
            if not tesla_df.empty:
                logging.info(f"\nVINs Tesla manquants ({len(tesla_df)}):")
                for vin in tesla_df['vin']:
                    logging.info(f"- {vin}")
                    
            if not other_df.empty:
                logging.info(f"\nAutres VINs manquants ({len(other_df)}):")
                for make_name, group in other_df.groupby('make'):
                    logging.info(f"\n{make_name.upper()} ({len(group)}):")
                    for vin in group['vin']:
                        logging.info(f"- {vin}")

            # Traiter les véhicules Tesla
            if not tesla_df.empty:
                logging.info(f"\nTraitement de {len(tesla_df)} véhicules Tesla manquants...")
                await tesla_main(tesla_df)

            # Traiter les autres véhicules
            if not other_df.empty:
                logging.info(f"\nTraitement de {len(other_df)} véhicules non-Tesla manquants...")
                await other_main(other_df)

            logging.info("\nTraitement terminé avec succès")
        else:
            if different_info_vins:
                logging.info(f"\nTous les VINs sont en production mais {len(different_info_vins)} ont des informations différentes")
            else:
                logging.info("Tous les VINs du CSV sont déjà présents en production avec les mêmes informations")

    except Exception as e:
        logging.error(f"Erreur lors du traitement: {str(e)}")
        logging.error("Erreur détaillée:", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(process_vehicles()) 

'''
