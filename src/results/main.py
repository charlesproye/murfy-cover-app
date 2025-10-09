from core.gsheet_utils import clean_gsheet
from results.forecast.price_predictor import train_and_save
from results.scoring.update_scoring import main as update_scoring
from results.trendline.main import update_trendline_model, update_trendline_oem
from results.update_available_data.main import update_available_data

if __name__ == "__main__":
    # Update available data
    update_available_data()
    # Front
    update_scoring()
    # Trendlines
    update_trendline_oem()
    update_trendline_model()
    # price forecast
    train_and_save("model_price.pkl")

