from src.results.scoring.update_scoring import main as update_scoring
from src.results.soh_scrapping.scrapping_aramis import main as scrapping_aramis
from src.results.soh_scrapping.scrapping_autospherre import main as scrapping_autospherre
from src.results.trendlines_results.trendlines import run_trendline_main
from src.results.trendlines_results.trendlines_excel import run_excel_trendlines
from src.core.gsheet_utils import clean_gsheet

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

