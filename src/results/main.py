from core.gsheet_utils import clean_gsheet
from results.forecast.price_predictor import train_and_save
from results.scoring.update_scoring import main as update_scoring
from results.soh_scrapping.scrapping_aramis import main as scrapping_aramis
from results.soh_scrapping.scrapping_autospherre import main as scrapping_autospherre
from results.trendline.main import update_trendline_model, update_trendline_oem

if __name__ == "__main__":
    # Front
    update_scoring()
    # Trendlines
    # First udpate the data
    scrapping_aramis()
    # scrapping_autospherre()
    # #Calculate Trendlines
    update_trendline_oem()
    update_trendline_model()
    # price forecast
    train_and_save("model_price.pkl")

