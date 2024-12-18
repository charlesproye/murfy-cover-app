import asyncio
import logging
import pandas as pd
import uuid
from core.sql_utils import get_connection
import os
import re

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
        file_path = os.path.join(current_dir, '..', 'data', 'fleet_info_13.csv')
        
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

def standardize_model_type(model: str, type_value: str, brand: str) -> tuple[str, str]:
    if not type_value:
        return model.lower(), None

    model = model.lower()
    type_value = type_value.lower()

    suffixes_to_remove = ['5d', '4d', '3d', 'auto', 'aut', 'actieauto', 'onze deal', 
                         'business', 'executive', 'edition', 'line', 'r-design']
    
    for suffix in suffixes_to_remove:
        type_value = type_value.replace(f" {suffix}", "")
    
    mappings = {
        'bmw': {
            'i3': {  # Un seul modèle i3
                'patterns': [
                    (r'.*i3s.*120\s*ah.*', 'i3s 120ah'),  # i3s doit être avant i3 pour être vérifié en premier
                    (r'.*120\s*ah.*', '120ah'),           # i3 120ah standards
                    (r'.*94\s*ah.*|.*92\s*ah.*', '94ah'), # Capture les deux variantes 94/92ah
                ],
                'model_clean': lambda m: 'i3'
            },
            'i4': {
                'patterns': [(r'.*', 'x')],
                'model_clean': lambda m: 'i4'
            }
        },
        'ds': {
            'ds 3 crossback': {
                'patterns': [(r'.*e-tense 50\s*kwh.*', 'e-tense 50 kwh')],
            },
            'ds 7 crossback': {
                'patterns': [(r'.*e-tense 4x4.*', 'e-tense 4x4')],
            },
        },
        'mercedes': {
            'eqa': {
                'patterns': [(r'.*250.*|.*eqa.*', '250')],
                'model_clean': lambda m: 'eqa'
            },
            'eqb': {
                'patterns': [(r'.*250.*|.*eqb.*', '250')],
                'model_clean': lambda m: 'eqb'
            },
            'eqc': {
                # Tous les EQC sont des 400 4matic, peu importe les suffixes (amg, solution, luxury, etc.)
                'patterns': [(r'.*400.*4matic.*', '400 4matic')],
                'model_clean': lambda m: 'eqc'
            },
            'eqs': {
                # Tous les EQS sont des 450+, peu importe les suffixes (luxury, etc.)
                'patterns': [(r'.*450\+.*', '450+')],
                'model_clean': lambda m: 'eqs'
            },
            'sprinter': {
                'patterns': [(r'.*47kwh.*|.*sprinter.*', '47kwh')],
                'model_clean': lambda m: 'sprinter'
            },
            'vito': {
                'patterns': [(r'.*35kwh.*|.*vito.*', '35kwh')],
                'model_clean': lambda m: 'vito'
            },
        },
        'kia': {
            'e-niro': {
                'patterns': [(r'.*64kwh.*|.*x.*', '64kwh')],
            },
            'ev6': {
                'patterns': [(r'.*77\.4kwh.*rwd.*|.*x.*', '77.4kwh rwd')],
            },
        },
        'peugeot': {
            '208': {
                'patterns': [(r'.*50kwh 136.*', 'ev 50kwh 136')],
            },
            '2008': {
                'patterns': [(r'.*50kwh 136.*', 'ev 50kwh 136')],
                'model_clean': lambda m: '2008'  # Unifie e-2008 et 2008
            },
            'e-2008': {
                'patterns': [(r'.*50kwh 136.*', 'ev 50kwh 136')],
                'model_clean': lambda m: '2008'  # Unifie e-2008 et 2008
            },
        },
        'renault': {
            'zoe': {
                'patterns': [
                    (r'.*r110.*', 'R110'),
                    (r'.*r135.*', 'R135'),
                ],
                'model_clean': lambda m: 'zoe',
                'metadata': {
                    'url_image': {
                        'R110': 'https://olinn.eu/sites/default/files/styles/max_650/public/images/zoe-1-2.png?itok=WtFDoX9b',
                        'R135': 'https://carvo.ch/assets/images/models/md-5/medium/renault-zoe.png'
                    },
                    'warranty_km': 160000,
                    'warranty_date': {
                        'R110': 8,
                        'R135': 6
                    },
                    'capacity': {
                        'R110': 52,
                        'R135': 52
                    }
                }
            }
        },
        'ford': {
            'mustang mach-e': {
                'patterns': [
                    # 75 kWh versions
                    (r'.*75kwh.*awd.*(?:tech.*pack.*)?', '75kwh awd'),     # Ignore tech pack/plus
                    (r'.*75kwh.*rwd.*(?:tech.*pack.*)?', '75kwh rwd'),     # Ignore tech pack/plus
                    # 98 kWh versions
                    (r'.*98kwh.*extended.*range.*awd.*(?:tech.*pack.*)?', '98kwh extended range awd'),
                    (r'.*98kwh.*extended.*range.*rwd.*(?:tech.*pack.*)?', '98kwh extended range rwd'),
                ],
                'model_clean': lambda m: 'mustang mach-e'
            },
            'e-transit': {
                'patterns': [(r'.*', 'x')],
                'model_clean': lambda m: 'e-transit'
            }
        },
        'tesla': {
            'model 3': {
                'patterns': [
                    (r'.*standard range.*plus.*rear.?wheel.*|.*standard range.*plus.*rwd.*|.*rear.?wheel drive.*', 'RWD'),
                    (r'.*performance.*dual motor.*|.*performance.*', 'Performance'),
                    (r'.*long range.*all.?wheel drive.*', 'Long Range AWD'),
                ],
                'model_clean': lambda m: 'model 3',
                'metadata': {
                    'url_image': 'https://www.tesla.com/ownersmanual/images/GUID-B5641257-9E85-404B-9667-4DA5FDF6D2E7-online-en-US.png',
                    'warranty_km': 193000,
                    'warranty_date': 8,
                    'capacity': {
                        'RWD': 57.5,
                        'Performance': 75,
                        'Long Range AWD': 75
                    }
                }
            },
            'model s': {
                'patterns': [
                    (r'.*100d.*', '100D'),
                    (r'.*75d.*', '75D'),
                    (r'.*long range.*plus.*', 'Long Range Plus'),
                    (r'.*long range.*', 'Long Range'),
                    (r'.*plaid.*', 'Plaid'),
                    (r'.*performance.*', 'Performance'),
                    (r'.*standard range.*', 'Standard Range'),
                ],
                'model_clean': lambda m: 'model s',
                'metadata': {
                    'url_image': 'https://www.tesla.com/ownersmanual/images/GUID-5543BA62-932F-46C5-B1EF-44707D4726B2-online-en-US.png',
                    'warranty_km': 240000,
                    'warranty_date': 8,
                    'capacity': {
                        '100D': 100,
                        'Long Range': 95,
                        'Long Range Plus': 60,
                        'Performance': 73.5,
                        'Plaid': 95,
                        'Standard Range': 95
                    }
                }
            },
            'model x': {
                'patterns': [
                    (r'.*long range.*plus.*', 'Long Range Plus'),
                    (r'.*long range.*', 'Long Range'),
                ],
                'model_clean': lambda m: 'model x',
                'metadata': {
                    'url_image': 'https://www.tesla.com/ownersmanual/images/GUID-A016FC6C-5896-4495-9DD8-2B074869A838-online-en-US.png',
                    'warranty_km': 240000,
                    'warranty_date': 8,
                    'capacity': {
                        'Long Range': 95,
                        'Long Range Plus': 480
                    }
                }
            },
            'model y': {
                'patterns': [
                    (r'.*rear.?wheel drive.*', 'RWD'),
                    (r'.*long range.*rwd.*', 'Long Range RWD'),
                    (r'.*long range.*all.?wheel drive.*', 'Long Range AWD'),
                    (r'.*performance.*awd.*', 'Performance'),
                ],
                'model_clean': lambda m: 'model y',
                'metadata': {
                    'url_image': 'https://www.tesla.com/ownersmanual/images/GUID-1F2D8746-336F-4CF9-9A04-F35E960F31FE-online-en-US.png',
                    'warranty_km': 193000,
                    'warranty_date': 8,
                    'capacity': {
                        'RWD': 60,
                        'Long Range RWD': 81,
                        'Long Range AWD': 81,
                        'Performance': 81
                    }
                }
            }
        },
        'volvo': {
            'xc40': {
                'patterns': [
                    (r'.*p8.*awd.*', 'p8 awd'),
                    (r'.*recharge.*twin.*', 'recharge twin'),
                    (r'.*recharge.*plus.*', 'recharge plus'),
                    (r'.*recharge.*core.*', 'recharge core')
                ],
                'model_clean': lambda m: 'xc40',
                'metadata': {
                    'url_image': 'https://cas.volvocars.com/image/dynamic/MY25_2417/536/exterior-v1/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/default.png?market=fr&client=ccs-self-service&fallback&angle=4&w=750&bg=00000000',
                    'warranty_km': 160000,
                    'warranty_date': 8,
                    'capacity': {
                        'p8 awd': 75,
                        'recharge twin': 78,
                        'recharge plus': 69,
                        'recharge core': 69
                    }
                }
            }
        }
    }

    brand_lower = brand.lower()
    if brand_lower in mappings and model in mappings[brand_lower]:
        model_info = mappings[brand_lower][model]
        
        # Applique le nettoyage du modèle si spécifié
        if 'model_clean' in model_info:
            model = model_info['model_clean'](model)
            
        # Applique les patterns pour le type
        for pattern, replacement in model_info['patterns']:
            if re.search(pattern, type_value):
                return model, replacement

    return model, type_value.strip()

async def process_vehicles(df: pd.DataFrame):
    """Traite les véhicules du DataFrame et les insère dans la base de données"""
    BRAND_MAPPING = {
        'VOLVO': 'Volvo Cars'
    }

    COUNTRY_MAPPING = {
        'NL': 'Netherlands',
    }

    with get_connection() as conn:
        cursor = conn.cursor()
        
        for index, vehicle in df.iterrows():
            try:
                # Gestion de la marque
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
                
                # Standardisation du modèle et type
                model_name = vehicle['Model'].strip() if pd.notna(vehicle['Model']) else None
                type_value = vehicle['Type'].strip() if pd.notna(vehicle['Type']) else None

                if not model_name:
                    logging.error(f"Modèle manquant pour le véhicule VIN: {vehicle['VIN']}")
                    continue

                model_name, type_value = standardize_model_type(model_name, type_value, vehicle['brand'])

                # Recherche du modèle dans la base
                cursor.execute("""
                    SELECT id FROM vehicle_model 
                    WHERE LOWER(model_name) = %s 
                    AND (
                        (LOWER(type) = %s AND %s IS NOT NULL)
                        OR (type IS NULL AND %s IS NULL)
                    )
                    AND oem_id = %s
                """, (model_name.lower(), type_value.lower() if type_value else None, 
                     type_value, type_value, oem_id))
                
                result = cursor.fetchone()
                if result:
                    vehicle_model_id = result[0]
                else:
                    vehicle_model_id = str(uuid.uuid4())
                    if type_value:
                        cursor.execute("""
                            INSERT INTO vehicle_model (id, model_name, type, oem_id)
                            VALUES (%s, %s, %s, %s)
                            RETURNING id
                        """, (
                            vehicle_model_id,
                            model_name.lower(),
                            type_value.lower(),
                            oem_id
                        ))
                    else:
                        cursor.execute("""
                            INSERT INTO vehicle_model (id, model_name, oem_id)
                            VALUES (%s, %s, %s)
                            RETURNING id
                        """, (
                            vehicle_model_id,
                            model_name.lower(),
                            oem_id
                        ))
                    vehicle_model_id = cursor.fetchone()[0]
                    logging.info(f"Créé nouveau modèle: {model_name} {type_value or ''} pour {vehicle['brand']}")

                # Récupération fleet_id
                cursor.execute("""
                    SELECT id FROM fleet 
                    WHERE LOWER(fleet_name) = LOWER(%s)
                """, (vehicle['Ownership '],))
                
                fleet_result = cursor.fetchone()
                if not fleet_result:
                    logging.error(f"Fleet non trouvée pour ownership: {vehicle['Ownership ']}")
                    continue
                fleet_id = fleet_result[0]

                # Gestion de la région
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
                
                # Gestion du véhicule
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

async def get_existing_model_metadata():
    """Récupère les métadonnées existantes des modèles de véhicules"""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                o.oem_name,
                vm.model_name,
                vm.type,
                vm.url_image,
                vm.warranty_km,
                vm.warranty_date,
                vm.capacity
            FROM vehicle_model vm
            JOIN oem o ON vm.oem_id = o.id
            WHERE vm.url_image IS NOT NULL 
               OR vm.warranty_km IS NOT NULL 
               OR vm.warranty_date IS NOT NULL 
               OR vm.capacity IS NOT NULL
            ORDER BY o.oem_name, vm.model_name, vm.type
        """)
        
        results = cursor.fetchall()
        
        if results:
            print("\nMétadonnées existantes des modèles :")
            print("--------------------------------------------------------------------------------")
            print("Marque | Modèle | Type | URL | Garantie km | Garantie années | Capacité")
            print("--------------------------------------------------------------------------------")
            for row in results:
                oem, model, type_value, url, warranty_km, warranty_date, capacity = row
                print(f"{oem} | {model} | {type_value or 'N/A'} | {url or 'N/A'} | {warranty_km or 'N/A'} | {warranty_date or 'N/A'} | {capacity or 'N/A'}")
            print("--------------------------------------------------------------------------------")
        else:
            print("Aucune métadonnée trouvée dans la base")
            
        return results


async def list_used_models():
    """Liste tous les modèles de véhicules présents dans la base de données qui sont utilisés"""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                vm.id,
                o.oem_name,
                vm.model_name,
                vm.type,
                COUNT(v.id) as vehicle_count
            FROM vehicle_model vm
            JOIN oem o ON vm.oem_id = o.id
            JOIN vehicle v ON v.vehicle_model_id = vm.id
            GROUP BY vm.id, o.oem_name, vm.model_name, vm.type
            ORDER BY o.oem_name, vm.model_name, vm.type
        """)
        
        results = cursor.fetchall()
        
        if results:
            print("\nModèles de véhicules utilisés dans la base :")
            print("--------------------------------------------------------------------------------")
            print("ID | Marque | Modèle | Type | Nombre de véhicules")
            print("--------------------------------------------------------------------------------")
            total_vehicles = 0
            for row in results:
                model_id, oem, model, type_value, count = row
                type_str = type_value if type_value else "N/A"
                print(f"{model_id} | {oem} | {model} | {type_str} | {count}")
                total_vehicles += count
            print("--------------------------------------------------------------------------------")
            print(f"Total : {len(results)} modèles différents")
            print(f"Total véhicules : {total_vehicles}")
        else:
            print("Aucun modèle trouvé dans la base")

async def cleanup_unused_models():
    """
    Supprime les modèles de véhicules qui ne sont liés à aucun véhicule dans la base de données.
    Retourne le nombre de modèles supprimés.
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        
        try:
            # Récupère les modèles non utilisés
            cursor.execute("""
                SELECT 
                    vm.id,
                    o.oem_name,
                    vm.model_name,
                    vm.type
                FROM vehicle_model vm
                JOIN oem o ON vm.oem_id = o.id
                LEFT JOIN vehicle v ON v.vehicle_model_id = vm.id
                WHERE v.id IS NULL
            """)
            
            unused_models = cursor.fetchall()
            
            if unused_models:
                # Supprime les modèles non utilisés
                cursor.execute("""
                    DELETE FROM vehicle_model vm
                    WHERE NOT EXISTS (
                        SELECT 1 
                        FROM vehicle v 
                        WHERE v.vehicle_model_id = vm.id
                    )
                    RETURNING id
                """)
                
                deleted_count = len(cursor.fetchall())
                conn.commit()
                
                # Log les modèles supprimés
                logging.info(f"Suppression de {deleted_count} modèles non utilisés:")
                for model in unused_models:
                    model_id, oem, model_name, type_value = model
                    type_str = type_value if type_value else "N/A"
                    logging.info(f"- {oem} | {model_name} | {type_str} (ID: {model_id})")
                
                return deleted_count
            else:
                logging.info("Aucun modèle non utilisé trouvé dans la base")
                return 0
                
        except Exception as e:
            conn.rollback()
            logging.error(f"Erreur lors du nettoyage des modèles non utilisés: {str(e)}")
            raise

async def main():
    try:
        #on peut mettre ce qu'on veut ici
        ownership_filter = "Ayvens" 
        df = await read_fleet_info(ownership_filter)
        logging.info(f"Nombre total de véhicules dans fleet_info: {len(df)}")
        
        await process_vehicles(df)
        await list_used_models()
        await cleanup_unused_models()
        # metadata = await get_existing_model_metadata()
        
    except Exception as e:
        logging.error(f"Erreur dans le programme principal: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())

