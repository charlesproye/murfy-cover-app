import asyncio
import random
from datetime import datetime, timedelta, UTC
import uuid
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import OperationalError
from sqlalchemy import text
from asyncpg.exceptions import DeadlockDetectedError
import os
import math
from tqdm import tqdm

from transform.processed_results.demo_data.db import deps

async def get_session() -> AsyncSession:
    session = await deps.data_session()
    try:
        yield session
    finally:
        await session.close()

async def ingest_demo():
    print("Ingestion des données demo en cours...")
    async for session in get_session():
        try:
            # Récupérer les IDs des modèles Tisla
            tesla_query = text("""
                SELECT id FROM vehicle_model 
                WHERE LOWER(model_name) IN ('midel 3', 'midel y')
            """)
            tesla_result = await session.execute(tesla_query)
            model_id_tesla = [row[0] for row in tesla_result]
            await session.commit()

            # Récupérer l'ID du modèle Rinault
            renault_query = text("""
                SELECT id FROM vehicle_model 
                WHERE LOWER(model_name) = 'zoey'
            """)
            renault_result = await session.execute(renault_query)
            model_id_renault = [row[0] for row in renault_result]
            await session.commit()

            # Récupérer l'ID du modèle Fiord
            fiord_query = text("""
                SELECT id FROM vehicle_model 
                WHERE LOWER(model_name) = 'mistang mach-e'
            """)
            fiord_result = await session.execute(fiord_query)
            model_id_fiord = [row[0] for row in fiord_result]
            await session.commit()

            # Récupérer l'ID du modèle Pirsche
            pirsche_query = text("""
                SELECT id FROM vehicle_model 
                WHERE LOWER(model_name) = 'taycian'
            """)
            pirsche_result = await session.execute(pirsche_query)
            model_id_pirsche = [row[0] for row in pirsche_result]
            await session.commit()
            
            all_vehicle_models = model_id_tesla + model_id_renault + model_id_fiord + model_id_pirsche

            # Récupérer les IDs et noms des régions
            region_query = text("""
                SELECT id, LOWER(region_name) as region_name 
                FROM region 
                WHERE LOWER(region_name) IN ('ireland', 'germany', 'italy', 'france', 'norway')
            """)
            region_result = await session.execute(region_query)
            region_info = {row[0]: row[1] for row in region_result}
            region_id = list(region_info.keys())
            await session.commit()

            # Récupérer les IDs des flottes
            fleet_query = text("""
                SELECT id FROM fleet 
                WHERE LOWER(fleet_name) IN ('démo 1', 'démo 2')
            """)
            fleet_result = await session.execute(fleet_query)
            fleet_ids = [row[0] for row in fleet_result]
            await session.commit()

            vehicle_inserts = []
            vehicle_data_inserts = []

            def generate_vin():
                prefixes = {
                    'tisla': 'XP7',
                    'rinault': 'VF1',
                    'fiord': 'YV1',
                    'pirsche': 'WP0'
                }
                
                # Sélectionner le préfixe en fonction du modèle
                if model_id in model_id_tesla:
                    prefix = prefixes['tisla']
                elif model_id in model_id_renault:
                    prefix = prefixes['rinault']
                elif model_id in model_id_fiord:
                    prefix = prefixes['fiord']
                else:
                    prefix = prefixes['pirsche']
                
                # Générer la partie alphanumérique du milieu (2 caractères)
                middle = ''.join(random.choices('ABCDEFGHJKLMNPRSTUVWXYZ', k=2))
                
                # Générer la partie numérique (11 chiffres)
                numbers = ''.join(str(random.randint(0, 9)) for _ in range(11))
                
                return f"{prefix}{middle}{numbers}"

            def generate_vehicle_data(start_date, model_id, is_anomalous, region):
                data_points = []
                current_soh = 1.0
                current_odometer = 0.0
                current_cycles = 0
                
                # Définir les paramètres selon le modèle
                if model_id in model_id_tesla:
                    yearly_degradation = random.uniform(0.02, 0.03)
                    daily_km = random.uniform(130, 160)
                    consumption_base = random.uniform(18, 22)  # kWh/100km pour Tisla
                    cycles_per_week = random.uniform(4, 6)
                elif model_id in model_id_renault:
                    yearly_degradation = random.uniform(0.04, 0.05)
                    daily_km = random.uniform(90, 120)
                    consumption_base = random.uniform(16, 19)  # kWh/100km pour Rinault
                    cycles_per_week = random.uniform(3, 5)
                elif model_id in model_id_fiord:
                    yearly_degradation = random.uniform(0.03, 0.04)
                    daily_km = random.uniform(110, 140)
                    consumption_base = random.uniform(20, 24)  # kWh/100km pour Fiord
                    cycles_per_week = random.uniform(3, 5)
                else:  # Pirsche
                    yearly_degradation = random.uniform(0.02, 0.03)
                    daily_km = random.uniform(120, 150)
                    consumption_base = random.uniform(19, 23)  # kWh/100km pour Pirsche
                    cycles_per_week = random.uniform(4, 6)

                # Ajuster selon la région
                region_name = region_info[region]
                if region_name == 'ireland':
                    yearly_degradation *= 0.85
                    consumption_base *= 0.95  # Meilleure efficacité
                elif region_name == 'germany':
                    yearly_degradation *= 1.2
                    consumption_base *= 1.15  # Consommation plus élevée
                elif region_name == 'italy':
                    yearly_degradation *= 1.0
                    consumption_base *= 1.0
                elif region_name == 'france':
                    yearly_degradation *= 1.15
                    consumption_base *= 1.1
                else:  # norway
                    yearly_degradation *= 1.1
                    consumption_base *= 1.2

                if is_anomalous:
                    yearly_degradation *= random.uniform(1.2, 1.4)
                    daily_km *= random.uniform(0.6, 1.4)
                    consumption_base *= random.uniform(1.1, 1.3)
                    cycles_per_week *= random.uniform(1.2, 1.5)

                weekly_degradation = yearly_degradation / 52
                current_date = start_date + timedelta(days=7)
                end_date = datetime.now(UTC)
                
                while current_date <= end_date:
                    degradation = weekly_degradation * (1 + random.uniform(-0.1, 0.1))
                    current_soh = max(0.7, current_soh - degradation)
                    
                    if random.random() < 0.007:  # Réduit de 0.01 à 0.005
                        current_soh = max(0.7, current_soh - random.uniform(0.01, 0.03))  # Réduit de 0.02-0.05 à 0.01-0.03
                    
                    weekly_km = daily_km * 7 * (1 + random.uniform(-0.2, 0.2))
                    current_odometer += max(0, weekly_km)
                    
                    # Calculer les cycles de la semaine
                    weekly_cycles = cycles_per_week * (1 + random.uniform(-0.2, 0.2))
                    current_cycles += weekly_cycles
                    
                    # Calculer la consommation avec variation saisonnière
                    season_factor = 1 + 0.15 * math.sin(2 * math.pi * current_date.timetuple().tm_yday / 365)
                    current_consumption = consumption_base * season_factor * (1 + random.uniform(-0.1, 0.1))
                    
                    data_points.append({
                        'timestamp': current_date,
                        'soh': round(current_soh, 3),
                        'odometer': round(current_odometer + 100, 1),
                        'cycles': round(current_cycles, 0),
                        'consumption': round(current_consumption, 1)
                    })
                    
                    current_date += timedelta(days=7)
                
                return data_points
            
            # Générer 1000 véhicules
            for _ in range(1000):
                vehicle_id = str(uuid.uuid4())
                model_id = random.choice(all_vehicle_models)
                region = random.choice(region_id)  # Sélectionner la région
                is_anomalous = random.random() < 0.10
                
                # Générer la start_date
                start_date = datetime.now(UTC) - timedelta(days=random.randint(365, 4 * 365))
                
                vehicle_insert = f"""
                INSERT INTO vehicle (id, fleet_id, region_id, vehicle_model_id, vin, activation_status, 
                                   start_date, licence_plate, end_of_contract_date, is_displayed)
                VALUES (
                    '{vehicle_id}',
                    '{random.choice(fleet_ids)}',
                    '{region}',
                    '{model_id}',
                    '{generate_vin()}',
                    {random.choice(['true', 'false'])},
                    '{start_date.date()}',
                    'ABC-{random.randint(1000, 9999)}',
                    current_date + interval '{random.randint(0, 730)} days',
                    true
                );"""
                vehicle_inserts.append(vehicle_insert)

                vehicle_data = generate_vehicle_data(start_date, model_id, is_anomalous, region)
                
                # Créer les inserts SQL
                for data in vehicle_data:
                    if random.random() < 0.7:  # 70% de chance d'enregistrer un point de données
                        vehicle_data_insert = f"""
                        INSERT INTO vehicle_data (id, vehicle_id, odometer, soh, cycles, consumption, timestamp)
                        VALUES (
                            '{str(uuid.uuid4())}',
                            '{vehicle_id}',
                            {data['odometer']},
                            {data['soh']},
                            {data['cycles']},
                            {data['consumption']},
                            '{data['timestamp']}'
                        );"""
                        vehicle_data_inserts.append(vehicle_data_insert)
            
            BATCH_SIZE = 10  # Ajuster selon les besoins
            MAX_CONCURRENT_BATCHES = 5  # Nombre maximum de lots traités en parallèle

            async def insert_vehicle_batch(batch):
                async for session in get_session():
                    try:
                        for insert in batch:
                            await session.execute(text(insert))
                        await session.commit()
                    except Exception as e:
                        await session.rollback()
                        raise e

            async def insert_vehicle_data_batch(batch):
                async for session in get_session():
                    try:
                        for insert in batch:
                            await session.execute(text(insert))
                        await session.commit()
                    except Exception as e:
                        await session.rollback()
                        raise e

            print("Insertion des véhicules...")
            vehicle_batches = [vehicle_inserts[i:i + BATCH_SIZE] for i in range(0, len(vehicle_inserts), BATCH_SIZE)]
            for i in tqdm(range(0, len(vehicle_batches), MAX_CONCURRENT_BATCHES)):
                current_batches = vehicle_batches[i:i + MAX_CONCURRENT_BATCHES]
                await asyncio.gather(*[insert_vehicle_batch(batch) for batch in current_batches])
            
            print("\nInsertion des données véhicules...")
            vehicle_data_batches = [vehicle_data_inserts[i:i + BATCH_SIZE] for i in range(0, len(vehicle_data_inserts), BATCH_SIZE)]
            for i in tqdm(range(0, len(vehicle_data_batches), MAX_CONCURRENT_BATCHES)):
                current_batches = vehicle_data_batches[i:i + MAX_CONCURRENT_BATCHES]
                await asyncio.gather(*[insert_vehicle_data_batch(batch) for batch in current_batches])

            print(f"\nDonnées insérées en base : {len(vehicle_inserts)} véhicules et {len(vehicle_data_inserts)} points de données")

            current_dir = os.path.dirname(os.path.abspath(__file__))
            file_path = os.path.join(current_dir, 'insert_demo_data.sql')
            
            with open(file_path, 'w') as f:
                f.write('BEGIN;\n\n')
                f.write('\n'.join(vehicle_inserts))
                f.write('\n\n')
                f.write('\n'.join(vehicle_data_inserts))
                f.write('\n\nCOMMIT;')
            
            print(f"Fichier SQL généré : {file_path}")

        except Exception as e:
            await session.rollback()
            raise e
        finally:
            await session.close()

async def remove_demo_data():
    async for session in get_session():
        try:
            # Récupérer les IDs des flottes demo
            fleet_query = text("""
                SELECT id FROM fleet 
                WHERE LOWER(fleet_name) IN ('démo 1', 'démo 2')
            """)
            fleet_result = await session.execute(fleet_query)
            fleet_ids = [row[0] for row in fleet_result]
            await session.commit()
            
            if not fleet_ids:
                print("Aucune flotte demo trouvée à supprimer")
                return

            async with session.begin():
                # Supprimer d'abord les données des véhicules
                await session.execute(text("""
                    DELETE FROM vehicle_data 
                    WHERE vehicle_id IN (
                        SELECT id FROM vehicle 
                        WHERE fleet_id = ANY(:fleet_ids)
                    )
                """), {"fleet_ids": fleet_ids})
                
                # Puis supprimer les véhicules
                await session.execute(text("""
                    DELETE FROM vehicle 
                    WHERE fleet_id = ANY(:fleet_ids)
                """), {"fleet_ids": fleet_ids})
                
                await session.commit()
                print(f"Données demo supprimées pour {len(fleet_ids)} flottes")

        except Exception as e:
            await session.rollback()
            raise e
        finally:
            await session.close()

async def update_soh_comparison():
    async for session in get_session():
        print("Début du processus de mise à jour...")
        
        # Suppression de la table temporaire si elle existe déjà
        print("Nettoyage des tables temporaires existantes...")
        await session.execute(text("DROP TABLE IF EXISTS oem_trendlines;"))
        await session.commit()

        # Création de la table temporaire avec la régression linéaire par OEM
        print("Création de la table temporaire des trendlines...")
        trendline_query = text("""
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
        """)
        await session.execute(trendline_query)
        await session.commit()
        print("Table des trendlines créée avec succès.")

        # Mise à jour par oem_id
        oem_query = text("""
            SELECT DISTINCT vm.oem_id 
            FROM vehicle v
            JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
            ORDER BY vm.oem_id;
        """)
        oems = await session.execute(oem_query)
        oem_ids = [row[0] for row in oems]
        
        total_updated = 0
        print(f"Mise à jour pour {len(oem_ids)} marques de véhicules...")
        
        for oem_id in oem_ids:
            print(f"\nTraitement de la marque {oem_id}")
            
            ids_query = text("""
                SELECT DISTINCT vd.id
                FROM vehicle_data vd
                JOIN vehicle v ON vd.vehicle_id = v.id
                JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
                WHERE vm.oem_id = :oem_id 
                ORDER BY vd.id;
            """)
            
            result = await session.execute(ids_query, {"oem_id": oem_id})
            ids_to_update = [row[0] for row in result.fetchall()]
            
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
                        update_query = text("""
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
                                WHERE vd.id = ANY(:batch_ids)
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
                        """)
                        
                        result = await session.execute(update_query, {"batch_ids": batch_ids})
                        updated_rows = len(result.fetchall())
                        await session.commit()
                        
                        total_updated += updated_rows
                        print(f"Batch {i//batch_size + 1}/{(len(ids_to_update) + batch_size - 1)//batch_size}: {updated_rows} lignes mises à jour")
                        break
                        
                    except (OperationalError, DeadlockDetectedError) as e:
                        retry_count += 1
                        if retry_count >= max_retries:
                            print(f"Échec après {max_retries} tentatives pour le batch {i//batch_size + 1}")
                            raise
                        print(f"Tentative {retry_count}/{max_retries} échouée, nouvelle tentative...")
                        await asyncio.sleep(1)
                        await session.rollback()
        
        print("\nNettoyage de la table temporaire...")
        await session.execute(text("DROP TABLE IF EXISTS oem_trendlines;"))
        await session.commit()
        
        print(f"\nMise à jour terminée! {total_updated} lignes mises à jour au total.")

async def calculate_soh_comparison_thresholds(fleet_id: str = "1bd65dab-59fd-4d9b-9aee-f35c2a75b123"):
    """
    Calcule les seuils de soh_comparison pour obtenir les proportions suivantes :
    A: top 2.5% (97.5 percentile)
    B: next 12.5% (85-97.5 percentile)
    C: next 35% (50-85 percentile)
    D: next 35% (15-50 percentile)
    E: next 12.5% (2.5-15 percentile)
    F: bottom 2.5% (0-2.5 percentile)
    """
    async for session in get_session():
        query = text("""
            WITH latest_data AS (
                SELECT DISTINCT ON (vehicle_id) 
                    vehicle_id,
                    soh_comparison
                FROM vehicle_data
                WHERE soh_comparison IS NOT NULL
                ORDER BY vehicle_id, timestamp DESC
            )
            SELECT 
                percentile_cont(0.975) WITHIN GROUP (ORDER BY soh_comparison) as p97_5,
                percentile_cont(0.85) WITHIN GROUP (ORDER BY soh_comparison) as p85,
                percentile_cont(0.50) WITHIN GROUP (ORDER BY soh_comparison) as p50,
                percentile_cont(0.15) WITHIN GROUP (ORDER BY soh_comparison) as p15,
                percentile_cont(0.025) WITHIN GROUP (ORDER BY soh_comparison) as p2_5,
                COUNT(*) as total_count
            FROM latest_data
            JOIN vehicle v ON v.id = vehicle_id
            WHERE v.fleet_id = :fleet_id;
        """)
        
        result = await session.execute(query, {"fleet_id": fleet_id})
        thresholds = result.fetchone()
        
        if thresholds:
            print("\nSeuils de soh_comparison pour la classification :")
            print(f"Total véhicules analysés : {thresholds.total_count}")
            print("\nGrade | Seuil | Proportion")
            print("-" * 30)
            print(f"A     | > {thresholds.p97_5:.3f} | top 2.5%")
            print(f"B     | > {thresholds.p85:.3f}  | next 12.5%")
            print(f"C     | > {thresholds.p50:.3f}  | next 35%")
            print(f"D     | > {thresholds.p15:.3f}  | next 35%")
            print(f"E     | > {thresholds.p2_5:.3f}  | next 12.5%")
            print(f"F     | ≤ {thresholds.p2_5:.3f}  | bottom 2.5%")
        else:
            print("Aucune donnée trouvée pour cette flotte")

if __name__ == "__main__":
    asyncio.run(ingest_demo())
