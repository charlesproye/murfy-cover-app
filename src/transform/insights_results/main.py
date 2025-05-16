from transform.insights_results.update_scoring import main 
from transform.insights_results.trendlines import run_trendline_main 
from transform.insights_results.trendlines_excel import run_excel_trendlines 



if __name__ == "__main__":
    main()
    run_excel_trendlines()
    run_trendline_main()
