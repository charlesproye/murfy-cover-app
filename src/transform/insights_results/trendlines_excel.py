import gspread
from google.oauth2.service_account import Credentials
from transform.insights_results.trendlines import *
import logging

root_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
cred_path = os.path.join(root_dir, "ingestion", "vehicle_info", "config", "config.json")
CREDS = Credentials.from_service_account_file(
    cred_path,
    scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
)
client = gspread.authorize(CREDS)
logging.basicConfig(level=logging.INFO)

# Lecture de la google sheet avec les data
sheet = client.open("202505 - Courbes SoH")
courbes_sheet = sheet.worksheet("Courbes OS")
sheet_data = np.array(courbes_sheet.get_all_values())


# Data process
df_sheet = pd.DataFrame(sheet_data[1:,:6], columns=sheet_data[0,:6])
df_sheet["Odomètre (km)"] = df_sheet["Odomètre (km)"].apply(lambda x: x.replace(" ", "")).astype(int)
df_sheet["SoH"] = df_sheet["SoH"].apply(lambda x: x.replace("%", "")).astype(float)
df_sheet.rename(columns={"Odomètre (km)": "odometer",
                                "SoH": "soh"}, inplace=True)
# model 
for model in df_sheet.Modèle.unique():
    with engine.connect() as connection:
        query = text("""
                     SELECT *  
                     FROM vehicle_model vm
                     join oem o
                     on o.id = vm.oem_id
                     WHERE vm.model_name = :model
            """)
        dbeaver_df = pd.read_sql(query, connection, params={"model": model})
    if True in dbeaver_df['trendline_active'].values:
        df_model = df_sheet[df_sheet["Modèle"] == model]
        logging.info(f"Processing trendline for model: {model}")
        get_trendlines(df_model, version=model, update=True)
        
        # ecriture dans la gsheet de trendlines
        df_to_write = dbeaver_df[['oem_name', 'model_name', 'type']].iloc[[-1]]
        df_to_write['version'] = "None"
        df_to_write['source'] = "excel"
        sheet_out = client.open("BP - Rapport Freemium")
        worksheet = sheet_out.worksheet("Trendline")
        worksheet.append_rows(df_to_write.values.tolist())
        logging.info(f"Trendline written to sheet for model: {model}")


