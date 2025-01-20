from sqlalchemy.exc import OperationalError

from core.sql_utils import get_connection
from sqlalchemy import text

class FrontUtils:

    @staticmethod
    def update_scoring():
        with get_connection() as conn:
            cursor = conn.cursor()
            print("Début du processus de mise à jour...")
            cursor.execute("SELECT 1")
            # Test if tables exist
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            tables = cursor.fetchall()
            print("Available tables:", [table[0] for table in tables])
                
        # Suppression de la table temporaire si elle existe déjà
            print("Nettoyage des tables temporaires existantes...")
            cursor.execute("DROP TABLE IF EXISTS model_trendlines;")
            conn.commit()
            # Création de la table temporaire avec la régression linéaire par modèle
            print("Création de la table temporaire des trendlines...")
            trendline_query = """
                CREATE TEMP TABLE model_trendlines AS
                WITH model_stats AS (
                    SELECT 
                        v.vehicle_model_id,
                        vd.odometer,
                        vd.soh,
                        COUNT(*) OVER (PARTITION BY v.vehicle_model_id) as n,
                        AVG(vd.odometer) OVER (PARTITION BY v.vehicle_model_id) as avg_odometer,
                        AVG(vd.soh) OVER (PARTITION BY v.vehicle_model_id) as avg_soh
                    FROM vehicle_data vd
                    JOIN vehicle v ON v.id = vd.vehicle_id
                    WHERE vd.odometer > 0
                )
                SELECT 
                    vehicle_model_id,
                    -- Calcul du taux de dégradation moyen (perte de SOH par km)
                    COALESCE(
                        (1 - AVG(soh)) / NULLIF(AVG(odometer), 0),
                        0
                    ) as degradation_rate
                FROM model_stats
                GROUP BY vehicle_model_id;
            """
            cursor.execute(trendline_query)
            conn.commit()
            print("Table des trendlines créée avec succès.")

            # Mise à jour par model_id
            model_query ="""
                SELECT  DISTINCT vehicle_model_id 
                FROM vehicle 
                ORDER BY vehicle_model_id;
            """
            models = cursor.execute(model_query)
            models = cursor.fetchall()        
            model_ids = [row[0] for row in models]
    
            total_updated = 0
            print(f"Mise à jour pour {len(model_ids)} modèles de véhicules...")
            
            for model_id in model_ids:
                print(f"\nTraitement du modèle {model_id}")
                cursor.execute("""
                        SELECT DISTINCT vd.id
                    FROM vehicle_data vd
                    JOIN vehicle v ON vd.vehicle_id = v.id
                    WHERE v.vehicle_model_id = '000261a7-410c-43ea-a12c-a23a4b90a36d'
                    ORDER BY vd.id;
                """, (model_id,))
                result = cursor.fetchall()
                ids_to_update = [row[0] for row in result]
                
                if not ids_to_update:
                    print(f"Aucune donnée à mettre à jour pour ce modèle")
                    continue
                    
                print(f"Nombre de lignes à traiter : {len(ids_to_update)}")
                batch_size = 100
                
                for i in range(0, len(ids_to_update), batch_size):
                    batch_ids = ids_to_update[i:i + batch_size]
                    max_retries = 3
                    retry_count = 0
                    
                    while retry_count < max_retries:
                        try:
                            cursor.execute("""
                                WITH to_update AS (
                                    SELECT 
                                        vd.id,
                                        vd.soh,
                                        vd.odometer,
                                        mt.degradation_rate,
                                        -- Calcul du taux de dégradation réel du véhicule
                                        CASE 
                                            WHEN vd.odometer = 0 THEN 0
                                            ELSE (1 - vd.soh) / vd.odometer
                                        END as vehicle_degradation
                                    FROM vehicle_data vd
                                    JOIN vehicle v ON vd.vehicle_id = v.id
                                    JOIN model_trendlines mt ON v.vehicle_model_id = mt.vehicle_model_id
                                    WHERE vd.id = ANY(%s)
                                    FOR UPDATE OF vd SKIP LOCKED
                                )
                                UPDATE vehicle_data
                                SET soh_comparison = ROUND(
                                    CASE 
                                        WHEN tu.odometer = 0 THEN 1
                                        ELSE (
                                            -- Si le véhicule se dégrade plus vite que la moyenne, la valeur sera < 1
                                            -- Si le véhicule se dégrade moins vite que la moyenne, la valeur sera > 1
                                            CASE 
                                                WHEN tu.degradation_rate = 0 THEN 1
                                                ELSE 2 - (tu.vehicle_degradation / NULLIF(tu.degradation_rate, 0))
                                            END
                                        )
                                    END::numeric,
                                    3
                                )
                                FROM to_update tu
                                WHERE vehicle_data.id = tu.id
                                RETURNING vehicle_data.id;
                            """, (batch_ids,))
                            result = cursor.fetchall()
                            updated_rows = len(result)
                            conn.commit()
                            
                            total_updated += updated_rows
                            print(f"Batch {i//batch_size + 1}/{(len(ids_to_update) + batch_size - 1)//batch_size}: {updated_rows} lignes mises à jour")
                            break
                            
                        except (OperationalError) as e:
                            retry_count += 1
                            if retry_count >= max_retries:
                                print(f"Échec après {max_retries} tentatives pour le batch {i//batch_size + 1}")
                                raise
                            print(f"Tentative {retry_count}/{max_retries} échouée, nouvelle tentative...")
                            conn.rollback()
        
            print("\nNettoyage de la table temporaire...")
            with conn.cursor() as cursor:
                cursor.execute("""DROP TABLE IF EXISTS model_trendlines;""")
                conn.commit()
            
        print(f"\nMise à jour terminée! {total_updated} lignes mises à jour au total.")

def main():
    # Get configuration from environment variables
    FrontUtils().update_scoring()


if __name__ == "__main__":
    main()

