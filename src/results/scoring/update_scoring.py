from logging import Logger, getLogger

import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.exc import OperationalError

from core.numpy_utils import numpy_safe_eval
from core.sql_utils import get_connection, get_sqlalchemy_engine
from db_models import Vehicle, VehicleData, VehicleModel
from results.trendline.main import compute_trendline_functions

LOGGER = getLogger(__name__)


def update_scoring(logger: Logger = LOGGER):
    with get_connection() as conn:
        cursor = conn.cursor()
        logger.info("Début du processus de mise à jour...")

        # Suppression de la table temporaire si elle existe déjà
        logger.info("Nettoyage des tables temporaires existantes...")
        cursor.execute("DROP TABLE IF EXISTS oem_trendlines;")
        conn.commit()

        # Création de la table temporaire avec la régression linéaire par OEM
        logger.info("Création de la table temporaire des trendlines...")
        trendline_query = """
            CREATE TEMP TABLE oem_trendlines AS
            WITH oem_stats AS (
                SELECT
                    vm.oem_id,
                    vd.odometer,
                    vd.soh_bib,
                    COUNT(*) OVER (PARTITION BY vm.oem_id) as n,
                    AVG(vd.odometer) OVER (PARTITION BY vm.oem_id) as avg_odometer,
                    AVG(vd.soh_bib) OVER (PARTITION BY vm.oem_id) as avg_soh,
                    STDDEV(vd.soh_bib) OVER (PARTITION BY vm.oem_id) as stddev_soh
                FROM vehicle_data vd
                JOIN vehicle v ON v.id = vd.vehicle_id
                JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
                WHERE vd.odometer > 0
            )
            SELECT
                oem_id,
                -- Calcul du taux de dégradation moyen (perte de SOH par km)
                COALESCE(
                    NULLIF((1 - AVG(soh_bib)) / NULLIF(AVG(odometer), 0), 0),
                    0.00001
                ) as degradation_rate,
                AVG(stddev_soh) as stddev_soh
            FROM oem_stats
            GROUP BY oem_id;
        """
        cursor.execute(trendline_query)
        conn.commit()
        logger.info("Table des trendlines créée avec succès.")

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
        logger.info(f"Mise à jour pour {len(oem_ids)} marques de véhicules...")

        for oem_id in oem_ids:
            cursor.execute(
                """
                SELECT DISTINCT vd.id
                FROM vehicle_data vd
                JOIN vehicle v ON vd.vehicle_id = v.id
                JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
                WHERE vm.oem_id = %s
                ORDER BY vd.id;
            """,
                (oem_id,),
            )

            ids_to_update = [row[0] for row in cursor.fetchall()]

            if not ids_to_update:
                logger.debug("Aucune donnée à mettre à jour pour cette marque")
                continue

            batch_size = 100

            for i in range(0, len(ids_to_update), batch_size):
                batch_ids = ids_to_update[i : i + batch_size]
                max_retries = 3
                retry_count = 0

                while retry_count < max_retries:
                    try:
                        cursor.execute(
                            """
                            WITH to_update AS (
                                SELECT
                                    vd.id,
                                    vd.soh_bib,
                                    vd.odometer,
                                    ot.degradation_rate,
                                    ot.stddev_soh,
                                    -- Calcul du taux de dégradation réel du véhicule
                                    CASE
                                        WHEN vd.odometer = 0 THEN 0
                                        ELSE (1 - vd.soh_bib) / NULLIF(vd.odometer, 0)
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
                                                            GREATEST(
                                                                LEAST(
                                                                    -2 * (tu.vehicle_degradation - tu.degradation_rate) / NULLIF(tu.degradation_rate, 0),
                                                                    99.999
                                                                ),
                                                                -99.999
                                                            )
                                                        -- Si dégradation plus lente que la moyenne
                                                        ELSE
                                                            GREATEST(
                                                                LEAST(
                                                                    3 * (tu.degradation_rate - tu.vehicle_degradation) / NULLIF(tu.degradation_rate, 0),
                                                                    99.999
                                                                ),
                                                                -99.999
                                                            )
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
                        """,
                            (batch_ids,),
                        )

                        updated_rows = len(cursor.fetchall())
                        conn.commit()

                        total_updated += updated_rows
                        logger.debug(
                            f"Batch {i // batch_size + 1}/{(len(ids_to_update) + batch_size - 1) // batch_size}: {updated_rows} lignes mises à jour"
                        )
                        break

                    except OperationalError:
                        retry_count += 1
                        if retry_count >= max_retries:
                            logger.debug(
                                f"Échec après {max_retries} tentatives pour le batch {i // batch_size + 1}"
                            )
                            raise
                        logger.error(
                            f"Tentative {retry_count}/{max_retries} échouée, nouvelle tentative..."
                        )
                        conn.rollback()

        logger.info("\nNettoyage de la table temporaire...")
        cursor.execute("DROP TABLE IF EXISTS oem_trendlines;")
        conn.commit()

        logger.info(
            f"\nMise à jour terminée! {total_updated} lignes mises à jour au total."
        )

        return {"rows_updated": total_updated, "oem_count": len(oem_ids)}


def eval_trendline_vectorized(expr, x):
    if expr is None:
        return np.full_like(x, np.nan, dtype=float)
    return numpy_safe_eval(expr, x=x)


def compute_bib_score(logger: Logger = LOGGER):
    stmt = (
        select(
            Vehicle.vin,
            VehicleModel.id.label("model_id"),
            VehicleData.soh_bib.cast(sa.Float).label("soh"),
            VehicleData.odometer.cast(sa.Float).label("odometer"),
            VehicleModel.trendline_bib,
            VehicleModel.trendline_bib_min,
            VehicleModel.trendline_bib_max,
        )
        .join(Vehicle, VehicleData.vehicle_id == Vehicle.id)
        .join(VehicleModel, Vehicle.vehicle_model_id == VehicleModel.id)
        .where(VehicleData.soh_bib.isnot(None))
        .where(VehicleData.odometer.isnot(None))
    )

    engine = get_sqlalchemy_engine()
    df = pd.read_sql_query(stmt, engine)

    results = []
    logger.info("Compute bib_score for each model...")
    for _model_id, df_model in df.groupby("model_id"):
        df_model = df_model.sort_values("odometer")
        soh = df_model["soh"].to_numpy()
        km = df_model["odometer"].to_numpy()

        categories = np.full(len(df_model), "C", dtype=object)

        trendline_mean = (
            df_model["trendline_bib"].dropna().iloc[0]
            if df_model["trendline_bib"].notna().any()
            else None
        )
        trendline_max = (
            df_model["trendline_bib_max"].dropna().iloc[0]
            if df_model["trendline_bib_max"].notna().any()
            else None
        )
        trendline_min = (
            df_model["trendline_bib_min"].dropna().iloc[0]
            if df_model["trendline_bib_min"].notna().any()
            else None
        )

        # No trendlines
        if trendline_mean is None:
            window = 100
            bounds = np.array(
                [
                    np.percentile(
                        soh[max(0, i - window) : min(len(soh), i + window)],
                        [5, 25, 75, 95],
                    )
                    for i in range(len(soh))
                ]
            )

            p5, p25, p75, p95 = bounds.T

            categories[soh >= p95] = "A"
            categories[(soh >= p75) & (soh < p95)] = "B"
            categories[(soh >= p25) & (soh < p75)] = "C"
            categories[(soh >= p5) & (soh < p25)] = "D"
            categories[soh < p5] = "E"

        # trendlines
        else:
            t95 = eval_trendline_vectorized(trendline_max, km)
            t5 = eval_trendline_vectorized(trendline_min, km)

            _, upper, lower = compute_trendline_functions(
                km, soh, distribution=1, interval=(5, 75)
            )

            t25 = eval_trendline_vectorized(lower, km)
            t75 = eval_trendline_vectorized(upper, km)

            categories[soh >= t95] = "A"
            categories[soh < t5] = "E"
            categories[(soh > t75) & (soh < t95)] = "B"
            categories[(soh < t25) & (soh >= t5)] = "D"

        df_model["bib_score"] = categories

        # Score minimal par VIN
        results.append(
            df_model.sort_values("soh")
            .groupby("vin", as_index=False)
            .first()[["vin", "bib_score"]]
        )

    final_df = pd.concat(results, ignore_index=True)

    # Update the bib_score in vehicle table
    logger.info("Update bib_score in vehicle table...")
    engine = get_sqlalchemy_engine()
    final_df[["vin", "bib_score"]].to_sql(
        "tmp_soh", engine, if_exists="replace", index=False
    )

    with get_connection() as con:
        cur = con.cursor()
        cur.execute("""
            UPDATE vehicle v
            SET bib_score = s.bib_score
            FROM tmp_soh s
            WHERE v.vin = s.vin
        """)
        # Drop temporary table
        cur.execute("DROP TABLE IF EXISTS tmp_soh;")
        con.commit()
    logger.info("Bib score updated successfully")


if __name__ == "__main__":
    update_scoring()
