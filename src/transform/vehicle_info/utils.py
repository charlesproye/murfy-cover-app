from rapidfuzz import process, fuzz
from core.sql_utils import *



def uniform_vehicules_type(type_car, oem_name, model_name, db_df, battery_capacity=None):
    """_summary_

    Args:
        type_car (str): type car to find match
        oem_name (str): oem car
        model_name (str): model car
        db_df (pd.DataFrame): db with all the model in dbeaver
        battery_capacity (str, optional): capacity car battery. Defaults to None.

    Returns:
        str: type le plus proche présent dans la db de vehicle_model
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
    oem_name = oem_name.lower()
    model_name = model_name.lower()
    type_car = type_car.lower()
    # filtre sur l'oem 
    subset = db_df[db_df['oem_name'] == oem_name].copy()
    # Trouver la meilleure correspondance
    # Retourne le modèle le plus proche score_cutoff fixé a 0 pour le moment pour être sur d'avoir un retour
    match_model = process.extractOne(model_name, subset['model_name'], scorer=fuzz.token_sort_ratio)
    if match_model :
        match_model_name, score, index = match_model
        # filtre sur le nom du modèle
        subset = subset[subset['model_name']==match_model_name]
        # on cherche la batetrie avec la capacité la + proche
        try:
            battery_target = float(battery_capacity.replace('kWh', '').replace('kwh', '').strip())
            subset["distance"] = (subset["capacity"] - battery_target).abs()
            min_distance = subset["distance"].min()
            closest_rows = subset[subset["distance"] == min_distance]
            # Si +sieurs batterie -> type le plus ressemblant
            match_type = process.extractOne(type_car, closest_rows['type'], scorer=fuzz.token_sort_ratio)
            match_model_type, score, index = match_type
            return closest_rows.loc[index, "type"]
        
        # type le plus ressemblant sans batterie 
        except:
            match_type = process.extractOne(type_car, subset['type'], scorer=fuzz.token_sort_ratio)
            match_model_type, score, index = match_type
            return subset.loc[index, "type"]
        
    else:
        return None

        
