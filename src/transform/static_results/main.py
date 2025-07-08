from transform.insights_results.update_scoring import main 
from transform.insights_results.trendlines import run_trendline_main
from transform.insights_results.trendlines_excel import run_excel_trendlines
from core.gsheet_utils import clean_gsheet

if __name__ == "__main__":
    # main()
    clean_gsheet("BP - Rapport Freemium", "Trendline")
    run_trendline_main()
    run_excel_trendlines()

