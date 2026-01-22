import logging

from sqlalchemy import text
from sqlalchemy.engine import Engine

from core.sql_utils import get_sqlalchemy_engine

COLUMNS_TO_CHECK = ["odometer", "soh_bib", "soh_oem"]
LOGGER = logging.getLogger(__name__)


def check_columns_from_model_name(model_name, column_list, engine):
    """Returns a list of column names from vehicle_data that contain at least one non-null value
    for vehicles associated with a specific vehicle model name.

    Parameters:
        model_name (str): The name of the vehicle model to filter on (vehicle_model.model_name).
        column_list (list of str): The list of column names (from vehicle_data) to check.
        engine (sqlalchemy.Engine): A SQLAlchemy engine instance used to execute the query.

    Returns:
        list of str: Column names from `column_list` that have at least one non-null value
                     for vehicles matching the given model_name.
    """
    query_parts = []
    params = {}

    for i, col in enumerate(column_list):
        col_param = f"model_name_{i}"
        params[col_param] = model_name
        part = f"""
        SELECT '{col}' AS column_available
        WHERE (
          SELECT COUNT(vd.{col})
          FROM vehicle v
          JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
          JOIN vehicle_data vd ON vd.vehicle_id = v.id
          WHERE vm.model_name = :{col_param}
        ) > 0
        """
        query_parts.append(part.strip())

    full_query = "\nUNION ALL\n".join(query_parts)
    with engine.begin() as conn:
        result = conn.execute(text(full_query), params)
        return [row[0] for row in result.fetchall()]


def update_model_columns_by_name(
    model_name, available_columns: list, all_columns: list, engine: Engine
):
    """
    Updates the boolean columns of vehicle_model with TRUE/FALSE
    based on their presence in available_columns for all models with the given name.

    Args:
        model_name (str): The name of the model to update.
        available_columns (List[str]): List of columns that are present.
        all_columns (List[str]): Complete list of boolean columns to update.
        engine (sqlalchemy.Engine): SQLAlchemy engine instance.
    """
    update_clauses = []
    params = {"model_name": model_name}

    for col in all_columns:
        param_name = f"{col}_value"
        update_clauses.append(
            f"{'soh' if col == 'soh_bib' else col}_data = :{param_name}"
        )
        params[param_name] = col in available_columns

    update_sql = f"""
        UPDATE vehicle_model
        SET {", ".join(update_clauses)}
        WHERE model_name = :model_name
    """

    with engine.begin() as conn:
        result = conn.execute(text(update_sql), params)
        return result.rowcount


def update_available_data(logger: logging.Logger = LOGGER):
    engine = get_sqlalchemy_engine()

    # Récupérer les noms de modèles uniques
    with engine.connect() as conn:
        result = conn.execute(text("SELECT DISTINCT model_name FROM vehicle_model"))
        model_names = [row[0] for row in result.fetchall()]

    models_with_data = 0
    column_counts = dict.fromkeys(COLUMNS_TO_CHECK, 0)
    total_models_updated = 0

    for model_name in model_names:
        try:
            available_columns = check_columns_from_model_name(
                model_name, COLUMNS_TO_CHECK, engine
            )
            rows_updated = update_model_columns_by_name(
                model_name, available_columns, COLUMNS_TO_CHECK, engine
            )
            total_models_updated += rows_updated

            if available_columns:
                models_with_data += 1
                for col in available_columns:
                    column_counts[col] += 1

            logger.info(f"✅ Updated {rows_updated} model(s) for name '{model_name}'")
        except Exception as e:
            logger.error(f"❌ Error for model name '{model_name}': {e}")

    return {
        "unique_model_names_processed": len(model_names),
        "model_names_with_data": models_with_data,
        "total_model_rows_updated": total_models_updated,
        "column_availability_counts": column_counts,
    }


if __name__ == "__main__":
    update_available_data()
