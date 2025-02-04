from sqlalchemy.exc import OperationalError

from core.sql_utils import get_connection
from sqlalchemy import text

class FrontUtils:

    @staticmethod
    def update_scoring():
        with get_connection() as conn:
            cursor = conn.cursor()
            print("Début du processus de mise à jour...")
            
            # Suppression de la table temporaire si elle existe déjà
            print("Nettoyage des tables temporaires existantes...")
            cursor.execute("DROP TABLE IF EXISTS oem_trendlines;")
            conn.commit()

            # Création de la table temporaire avec la régression linéaire par OEM
            print("Création de la table temporaire des trendlines...")
            trendline_query = """
                CREATE TEMP TABLE oem_trendlines AS
                WITH oem_stats AS (
                    SELECT 
                        vm.oem_id,
                        vd.odometer,
                        vd.soh,
                        COUNT(*) OVER (PARTITION BY vm.oem_id) as n,
                        AVG(vd.odometer) OVER (PARTITION BY vm.oem_id) as avg_odometer,
                        AVG(vd.soh) OVER (PARTITION BY vm.oem_id) as avg_soh,
                        STDDEV(vd.soh) OVER (PARTITION BY vm.oem_id) as stddev_soh
                    FROM vehicle_data vd
                    JOIN vehicle v ON v.id = vd.vehicle_id
                    JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
                    WHERE vd.odometer > 0
                )
                SELECT 
                    oem_id,
                    -- Calcul du taux de dégradation moyen (perte de SOH par km)
                    COALESCE(
                        NULLIF((1 - AVG(soh)) / NULLIF(AVG(odometer), 0), 0),
                        0.00001
                    ) as degradation_rate,
                    AVG(stddev_soh) as stddev_soh
                FROM oem_stats
                GROUP BY oem_id;
            """
            cursor.execute(trendline_query)
            conn.commit()
            print("Table des trendlines créée avec succès.")

            # Mise à jour par oem_id
            oem_query = """
                SELECT DISTINCT vm.oem_id 
                FROM vehicle v
                JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
                ORDER BY vm.oem_id;
            """
            cursor.execute(oem_query)
            oem_ids = [row[0] for row in cursor.fetchall()]
            
            total_updated = 0
            print(f"Mise à jour pour {len(oem_ids)} marques de véhicules...")
            
            for oem_id in oem_ids:
                print(f"\nTraitement de la marque {oem_id}")
                
                cursor.execute("""
                    SELECT DISTINCT vd.id
                    FROM vehicle_data vd
                    JOIN vehicle v ON vd.vehicle_id = v.id
                    JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
                    WHERE vm.oem_id = %s 
                    ORDER BY vd.id;
                """, (oem_id,))
                
                ids_to_update = [row[0] for row in cursor.fetchall()]
                
                if not ids_to_update:
                    print(f"Aucune donnée à mettre à jour pour cette marque")
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
                                        ot.degradation_rate,
                                        ot.stddev_soh,
                                        -- Calcul du taux de dégradation réel du véhicule
                                        CASE 
                                            WHEN vd.odometer = 0 THEN 0
                                            ELSE (1 - vd.soh) / NULLIF(vd.odometer, 0)
                                        END as vehicle_degradation
                                    FROM vehicle_data vd
                                    JOIN vehicle v ON vd.vehicle_id = v.id
                                    JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
                                    JOIN oem_trendlines ot ON vm.oem_id = ot.oem_id
                                    WHERE vd.id = ANY(%s)
                                    FOR UPDATE OF vd SKIP LOCKED
                                )
                                UPDATE vehicle_data
                                SET soh_comparison = ROUND(
                                    CASE 
                                        WHEN tu.odometer = 0 THEN 1
                                        ELSE (
                                            CASE 
                                                WHEN tu.degradation_rate = 0 THEN 1
                                                ELSE (
                                                    -- Normalisation basée sur l'écart à la moyenne
                                                    1 + (
                                                        CASE
                                                            -- Si dégradation plus rapide que la moyenne
                                                            WHEN tu.vehicle_degradation > tu.degradation_rate THEN
                                                                -2 * (tu.vehicle_degradation - tu.degradation_rate) 
                                                                / NULLIF(tu.degradation_rate, 0)
                                                            -- Si dégradation plus lente que la moyenne
                                                            ELSE
                                                                3 * (tu.degradation_rate - tu.vehicle_degradation) 
                                                                / NULLIF(tu.degradation_rate, 0)
                                                        END
                                                    )
                                                )
                                            END
                                        )
                                    END::numeric,
                                    3
                                )
                                FROM to_update tu
                                WHERE vehicle_data.id = tu.id
                                RETURNING vehicle_data.id;
                            """, (batch_ids,))
                            
                            updated_rows = len(cursor.fetchall())
                            conn.commit()
                            
                            total_updated += updated_rows
                            print(f"Batch {i//batch_size + 1}/{(len(ids_to_update) + batch_size - 1)//batch_size}: {updated_rows} lignes mises à jour")
                            break
                            
                        except OperationalError as e:
                            retry_count += 1
                            if retry_count >= max_retries:
                                print(f"Échec après {max_retries} tentatives pour le batch {i//batch_size + 1}")
                                raise
                            print(f"Tentative {retry_count}/{max_retries} échouée, nouvelle tentative...")
                            conn.rollback()
            
            print("\nNettoyage de la table temporaire...")
            cursor.execute("DROP TABLE IF EXISTS oem_trendlines;")
            conn.commit()
            
            print(f"\nMise à jour terminée! {total_updated} lignes mises à jour au total.")

def main():
    FrontUtils().update_scoring()

if __name__ == "__main__":
    main()
