import logging

from sqlalchemy import text
from sqlalchemy.engine import Engine

from core.sql_utils import get_sqlalchemy_engine

COLUMNS_TO_CHECK = ["odometer", "soh", "soh_oem"]
LOGGER = logging.getLogger(__name__)


def check_columns_from_model(model_id, column_list, engine):
    """Returns a list of column names from vehicle_data that contain at least one non-null value
    for vehicles associated with a specific vehicle model.

    Parameters:
        model_id (int): The ID of the vehicle model to filter on (vehicle_model.id).
        column_list (list of str): The list of column names (from vehicle_data) to check.
        engine (sqlalchemy.Engine): A SQLAlchemy engine instance used to execute the query.

    Returns:
        list of str: Column names from `column_list` that have at least one non-null value
                     for vehicles matching the given model_id.
    """
    query_parts = []
    params = {}

    for i, col in enumerate(column_list):
        col_param = f"model_id_{i}"
        params[col_param] = model_id
        part = f"""
        SELECT '{col}' AS column_available
        WHERE (
          SELECT COUNT(vd.{col})
          FROM vehicle v
          JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
          JOIN vehicle_data vd ON vd.vehicle_id = v.id
          WHERE vm.id = :{col_param}
        ) > 0
        """
        query_parts.append(part.strip())

    full_query = "\nUNION ALL\n".join(query_parts)
    with engine.begin() as conn:
        result = conn.execute(text(full_query), params)
        return [row[0] for row in result.fetchall()]


def update_model_columns(
    model_id, available_columns: list, all_columns: list, engine: Engine
):
    """
    Updates the boolean columns of vehicle_model with TRUE/FALSE
    based on their presence in available_columns.

    Args:
        model_id (str): The ID of the model to update.
        available_columns (List[str]): List of columns that are present.
        all_columns (List[str]): Complete list of boolean columns to update.
        engine (sqlalchemy.Engine): SQLAlchemy engine instance.
    """
    update_clauses = []
    params = {"model_id": model_id}

    for col in all_columns:
        param_name = f"{col}_value"
        update_clauses.append(f"{col}_data = :{param_name}")
        params[param_name] = col in available_columns

    update_sql = f"""
        UPDATE vehicle_model
        SET {", ".join(update_clauses)}
        WHERE id = :model_id
    """

    with engine.begin() as conn:
        conn.execute(text(update_sql), params)


def update_available_data(logger: logging.Logger = LOGGER):
    engine = get_sqlalchemy_engine()

    with engine.connect() as conn:
        result = conn.execute(text("SELECT id FROM vehicle_model"))
        model_ids = [row[0] for row in result.fetchall()]

    models_with_data = 0
    column_counts = dict.fromkeys(COLUMNS_TO_CHECK, 0)

    for model_id in model_ids:
        try:
            available_columns = check_columns_from_model(
                model_id, COLUMNS_TO_CHECK, engine
            )
            update_model_columns(model_id, available_columns, COLUMNS_TO_CHECK, engine)
            if available_columns:
                models_with_data += 1
                for col in available_columns:
                    column_counts[col] += 1
        except Exception as e:
            logger.error(f"❌ Error for {model_id}: {e}")

    return {
        "models_processed": len(model_ids),
        "models_with_data": models_with_data,
        "column_availability_counts": column_counts,
    }
