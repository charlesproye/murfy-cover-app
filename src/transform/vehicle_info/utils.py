from rapidfuzz import process, fuzz
from core.sql_utils import *



def uniform_vehicules_type(row, db_df):
    """Permet d'uniformiser les types de véhicules avec ceux présent dans la db 

    Args:
        row (pd.Series): avec les infos du vin min required column: oem, Modèle, Type 
        db_df (pd.DataFrame): dataframe avec les colonnes model_name, id, type, oem_name, capacity

    Returns:
        str: type du modèle présent sur dbeaver
    """

#__________ Faire tourner cette requête en dehors pour récupérer les infos nécessaires sur la db_________
# from core.sql_utils import *
# engine = get_sqlalchemy_engine()
# con = engine.connect()

# with engine.connect() as connection:
#     dbeaver_df = pd.read_sql(text("""SELECT vm.model_name, vm.id, vm.type, o.oem_name, b.capacity FROM vehicle_model vm
#                                   join OEM o on vm.oem_id=o.id
#                                   join battery b on b.id=vm.battery_id;"""), con)
#___________________________________________________________________________________________________________

    
    #On récupère les infos
    oem = row['OEM'].lower()
    model_target = row['Modèle'].lower()
    type_target = row['Type'].lower()
    # filtre sur l'oem 
    subset = db_df[db_df['oem_name'] == oem].copy()
    
    # Trouver la meilleure correspondance
    match_model = process.extractOne(model_target, subset['model_name'], scorer=fuzz.token_sort_ratio)
    if match_model :
        match_model_name, score, index = match_model
        # filtre sur le nom du modèle
        subset = subset[subset['model_name']==match_model_name]
        # on cherche la batetrie avec la capacité la + proche
        if row['battery_capacity'] != 'unknown':
            battery_target = float(row['battery_capacity'].replace('kWh', '').replace('kwh', '').strip())
            subset["distance"] = (subset["capacity"] - battery_target).abs()
            min_distance = subset["distance"].min()
            closest_rows = subset[subset["distance"] == min_distance]
            # Si +sieurs batterie -> type le plus ressemblant
            match_type = process.extractOne(type_target, closest_rows['type'], scorer=fuzz.token_sort_ratio)
            match_model_type, score, index = match_type
            return closest_rows.loc[index, "type"]

        # si on a pas de type dans dbeaver après le masking
        if subset['type'] is None:
            return None
        
        # type le plus ressemblant sans batterie 
        match_type = process.extractOne(type_target, subset['type'], scorer=fuzz.token_sort_ratio)
        match_model_type, score, index = match_type
        return subset.loc[index, "type"]

        
