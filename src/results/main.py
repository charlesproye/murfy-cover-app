from results.scoring.update_scoring import main as update_scoring
from results.soh_scrapping.scrapping_aramis import main as scrapping_aramis
from results.soh_scrapping.scrapping_autospherre import main as scrapping_autospherre
from core.gsheet_utils import clean_gsheet
from results.forecast.price_predictor import train_and_save

if __name__ == "__main__":
    #Front 
    update_scoring()
    #Trendlines
    #First udpate the data
    scrapping_aramis()
    # scrapping_autospherre()
    # #Calculate Trendlines
    # run_trendline_main()
    # run_excel_trendlines()
    # price forecast
    train_and_save('model_price.pkl')

