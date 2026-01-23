import logging

from results.forecast.price_predictor import train_and_save
from results.real_autonomy.calculate_real_autonomy import update_real_autonomy
from results.scoring.update_scoring import compute_bib_score
from results.trendline.main import main as update_trendlines
from results.update_available_data.main import update_available_data
from results.vehicle_status.main import run_vehicle_status_checks

LOGGER = logging.getLogger(__name__)


def results_pipeline(logger: logging.Logger = LOGGER):
    available_data_summary = update_available_data(logger=logger) or {}
    scoring_summary = compute_bib_score(logger=logger) or {}
    trendline_model_summary = update_trendlines() or {}
    price_forecast_summary = train_and_save("model_price.pkl", logger=logger) or {}
    vehicle_status_summary = run_vehicle_status_checks(logger=logger) or {}
    real_autonomy_summary = update_real_autonomy() or {}

    return {
        "available_data": available_data_summary,
        "scoring": scoring_summary,
        "trendline_model": trendline_model_summary,
        "price_forecast": price_forecast_summary,
        "vehicle_status": vehicle_status_summary,
        "real_autonomy": real_autonomy_summary,
    }
