import asyncio
from fleet_info import read_fleet_info as fleet_info
from other import main as other_main
from tesla import main as tesla_main

from core.sql_utils import get_connection
from fleet_info import read_fleet_info as fleet_info
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def list_used_models():
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

def cleanup_unused_models():
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

def find_models_needing_completion():
    """
    Identifier les modèles avec des valeurs NULL.
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                       id,
                       model_name,
                       type
            FROM vehicle_model
            WHERE 
                       id IS NULL 
                       OR model_name IS NULL
                       OR url_image IS NULL 
                       OR warranty_km IS NULL 
                       OR warranty_date IS NULL 
                       OR capacity IS NULL
                       OR net_capacity IS NULL
                       OR autonomy IS NULL
        """)
        
        results_completion = cursor.fetchall()
        
        if results_completion:
            print("\nModèles besoin d'être complétés:")
            print("--------------------------------------------------------------------------------")
            print("ID | Modèle | Type ")
            print("--------------------------------------------------------------------------------")
            for row in results_completion:
                id, model_name, type = row
                print(f"{id} | {model_name} | {type}")
            print("--------------------------------------------------------------------------------")
            print(f"Total : {len(results_completion)} modèles besoin d'être complétés")
        else:
            print("Tout les modèles sont complets")


if __name__ == "__main__":

    fleet_info = asyncio.run(fleet_info())
    print(fleet_info)
    # Get the correct info model for each brand 
    asyncio.run(tesla_main(fleet_info))
    #Get the information of the model based on the excel file 
    asyncio.run(other_main(fleet_info))

    cleanup_unused_models()
    list_used_models()
    find_models_needing_completion()
