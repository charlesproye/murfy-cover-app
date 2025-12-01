import logging

from results.forecast.price_predictor import train_and_save
from results.scoring.update_scoring import update_scoring
from results.trendline.main import update_trendline_model, update_trendline_oem
from results.update_available_data.main import update_available_data

LOGGER = logging.getLogger(__name__)


def results_pipeline(logger: logging.Logger = LOGGER):
    available_data_summary = update_available_data(logger=logger) or {}
    scoring_summary = update_scoring(logger=logger) or {}
    trendline_oem_summary = update_trendline_oem() or {}
    trendline_model_summary = update_trendline_model() or {}
    price_forecast_summary = train_and_save("model_price.pkl", logger=logger) or {}

    return {
        "available_data": available_data_summary,
        "scoring": scoring_summary,
        "trendline_oem": trendline_oem_summary,
        "trendline_model": trendline_model_summary,
        "price_forecast": price_forecast_summary,
    }
