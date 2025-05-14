import gspread
from google.oauth2.service_account import Credentials
from transform.insights_results.trendlines import *


CREDS = Credentials.from_service_account_file(
    cred_path,
    scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
)
client = gspread.authorize(CREDS)

sheet = client.open("202505 - Courbes SoH")
worksheets = sheet.worksheets()
coubes_sheet = sheet.worksheet("Courbes OS")

df_spreed_sheet = pd.DataFrame(coubes_sheet.get_all_values()[1:], columns=coubes_sheet.get_all_values()[0])
df_spreed_sheet["Odomètre (km)"] = df_spreed_sheet["Odomètre (km)"].apply(lambda x: x.replace(" ", "")).astype(int)
df_spreed_sheet["SoH"] = df_spreed_sheet["SoH"].apply(lambda x: x.replace("%", "")).astype(float)
df_spreed_sheet.rename(columns={"Odomètre (km)": "odometer",
                                "SoH": "soh"}, inplace=True)

for model in df_spreed_sheet.Modèle.unique():
    with engine.connect() as connection:
        query = text("""
                     SELECT *  
                     FROM vehicle_model vm
                     join oem o
                     on o.id = vm.oem_id
                     WHERE vm.model_name = :model
                     AND :model not like '%model%'
            """)
        dbeaver_df = pd.read_sql(query, connection, params={"model": model})
    if True in dbeaver_df['trendline_active'].values:
        df_model = df_spreed_sheet[df_spreed_sheet['Modèle'] == model]
        print(df_model.columns)
        get_trendlines(df_model, version=model, update=True, source='excel')
        logging.warning("Update google sheet")
        df_to_write = pd.DataFrame(dbeaver_df[['oem_name', 'model_name', 'type']].iloc[-1]).T
        df_to_write['source'] = "excel"
        df_to_write['version'] = "None"
        client = gspread.authorize(CREDS)
        sheet = client.open("BP - Rapport Freemium")
        worksheet = sheet.worksheet("Trendline")
        worksheet.append_rows(df_to_write.values.tolist())

