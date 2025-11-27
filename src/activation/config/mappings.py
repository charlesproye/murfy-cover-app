import re
from datetime import date

import pandas as pd
from rapidfuzz import fuzz, process

MAKE_MAPPING = {
    "mercedes-benz": "mercedes",
    "mercedes": "mercedes",
    "Mercedes": "mercedes",
    "Mercedes-Benz": "mercedes",
    "MERCEDES": "mercedes",
    "MERCEDES-BENZ": "mercedes",
    # bmw
    "BMW": "bmw",
    # high mobility
    "FORD": "ford",
    "KIA": "kia",
    "Kia": "kia",
    # stellantis
    "PEUGEOT": "peugeot",
    "Peugeot": "peugeot",
    "OPEL": "opel",
    "Opel": "opel",
    "FIAT": "fiat",
    "Fiat": "fiat",
    "Citroën": "citroën",
    "DS": "ds",
    # other
    "TESLA": "tesla",
    "Tesla": "tesla",
    "Renault": "renault",
    "RENAULT": "renault",
}

OEM_MAPPING = {
    "Mercedes": "mercedes",
    "MERCEDES": "mercedes",
    "mercedes-benz": "mercedes",
    "Mercedes-Benz": "mercedes",
    "MERCEDES-BENZ": "mercedes",
    "Stellantis": "stellantis",
    "STELLANTIS": "stellantis",
    "stellantis": "stellantis",
    "volvo": "volvo-cars",
    "VOLVO": "volvo-cars",
    "Volvo": "volvo-cars",
    "VOLVO-CARS": "volvo-cars",
    "Volvo-Cars": "volvo-cars",
}

COUNTRY_MAPPING = {
    "nl": "Netherlands",
    "fr": "France",
    "be": "Belgium",
    "de": "Germany",
    "lu": "Luxembourg",
    "es": "Spain",
    "it": "Italy",
    "pt": "Portugal",
    "gb": "United Kingdom",
    "uk": "United Kingdom",
}

COL_DTYPES = {
    "vin": str,
    "licence_plate": str,
    "make": str,
    "model": str,
    "type": str,
    "oem": str,
    "owner": str,
    "country": str,
    "activation": bool,
    "eligibility": bool,
    "real_activation": bool,
    "activation_error": str,
    "activation_date": "datetime64[ns]",
    "deactivation_date": "datetime64[ns]",
}

suffixes_to_remove = [
    "5d",
    "4d",
    "3d",
    "auto",
    "aut",
    "actieauto",
    "onze deal",
    "business",
    "executive",
    "edition",
    "line",
    "r-design",
]
mappings = {
    "bmw": {
        "i3": {  # Un seul modèle i3
            "patterns": [
                (
                    r".*i3s.*120\s*ah.*",
                    "i3s 120ah",
                ),  # i3s doit être avant i3 pour être vérifié en premier
                (r".*120\s*ah.*", "120ah"),  # i3 120ah standards
                (
                    r".*94\s*ah.*|.*92\s*ah.*",
                    "94ah",
                ),  # Capture les deux variantes 94/92ah
            ],
            "model_clean": lambda m: "i3",
        },
        "i4": {"patterns": [(r".*", "x")], "model_clean": lambda m: "i4"},
    },
    "ds": {
        "ds 3 crossback": {
            "patterns": [(r".*e-tense 50\s*kwh.*", "e-tense 50 kwh")],
        },
        "ds 7 crossback": {
            "patterns": [(r".*e-tense 4x4.*", "e-tense 4x4")],
        },
    },
    "mercedes": {
        "eqa": {
            "patterns": [(r".*250.*|.*eqa.*", "250")],
            "model_clean": lambda m: "eqa",
        },
        "eqb": {
            "patterns": [(r".*250.*|.*eqb.*", "250")],
            "model_clean": lambda m: "eqb",
        },
        "eqc": {
            # Tous les EQC sont des 400 4matic, peu importe les suffixes (amg, solution, luxury, etc.)
            "patterns": [(r".*400.*4matic.*", "400 4matic")],
            "model_clean": lambda m: "eqc",
        },
        "eqs": {
            # Tous les EQS sont des 450+, peu importe les suffixes (luxury, etc.)
            "patterns": [(r".*450\+.*", "450+")],
            "model_clean": lambda m: "eqs",
        },
        "sprinter": {
            "patterns": [(r".*47kwh.*|.*sprinter.*", "47kwh")],
            "model_clean": lambda m: "sprinter",
        },
        "vito": {
            "patterns": [(r".*35kwh.*|.*vito.*", "35kwh")],
            "model_clean": lambda m: "vito",
        },
    },
    "kia": {
        "e-niro": {
            "patterns": [(r".*64kwh.*|.*x.*", "64kwh")],
        },
        "ev6": {
            "patterns": [(r".*77\.4kwh.*rwd.*|.*x.*", "77.4kwh rwd")],
        },
    },
    "peugeot": {
        "208": {
            "patterns": [(r".*50kwh 136.*", "ev 50kwh 136")],
            "model_clean": lambda m: "208",  # Unifie e-2008 et 2008
        },
        "2008": {
            "patterns": [(r".*50kwh 136.*", "ev 50kwh 136")],
            "model_clean": lambda m: "2008",  # Unifie e-2008 et 2008
        },
        "e-2008": {
            "patterns": [(r".*50kwh 136.*", "ev 50kwh 136")],
            "model_clean": lambda m: "2008",  # Unifie e-2008 et 2008
        },
    },
    "renault": {
        "zoe": {
            "patterns": [
                (r".*r110.*", "R110"),
                (r".*r135.*", "R135"),
            ],
            "model_clean": lambda m: "zoe",
            "metadata": {
                "url_image": {
                    "R110": "https://olinn.eu/sites/default/files/styles/max_650/public/images/zoe-1-2.png?itok=WtFDoX9b",
                    "R135": "https://carvo.ch/assets/images/models/md-5/medium/renault-zoe.png",
                },
                "warranty_km": 160000,
                "warranty_date": {"R110": 8, "R135": 6},
                "capacity": {"R110": 52, "R135": 52},
            },
        }
    },
    "ford": {
        "mustang mach-e": {
            "patterns": [
                # 75 kWh versions
                (
                    r".*75kwh.*awd.*(?:tech.*pack.*)?",
                    "75kwh awd",
                ),  # Ignore tech pack/plus
                (
                    r".*75kwh.*rwd.*(?:tech.*pack.*)?",
                    "75kwh rwd",
                ),  # Ignore tech pack/plus
                # 98 kWh versions
                (
                    r".*98kwh.*extended.*range.*awd.*(?:tech.*pack.*)?",
                    "98kwh extended range awd",
                ),
                (
                    r".*98kwh.*extended.*range.*rwd.*(?:tech.*pack.*)?",
                    "98kwh extended range rwd",
                ),
            ],
            "model_clean": lambda m: "mustang mach-e",
        },
        "e-transit": {"patterns": [(r".*", "x")], "model_clean": lambda m: "e-transit"},
    },
    "volvo": {
        "xc40": {
            "patterns": [
                (r".*p8.*awd.*", "p8 awd"),
                (r".*recharge.*twin.*", "recharge twin"),
                (r".*recharge.*plus.*", "recharge plus"),
                (r".*recharge.*core.*", "recharge core"),
            ],
            "model_clean": lambda m: "xc40",
            "metadata": {
                "url_image": "https://cas.volvocars.com/image/dynamic/MY25_2417/536/exterior-v1/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/default.png?market=fr&client=ccs-self-service&fallback&angle=4&w=750&bg=00000000",
                "warranty_km": 160000,
                "warranty_date": 8,
                "capacity": {
                    "p8 awd": 75,
                    "recharge twin": 78,
                    "recharge plus": 69,
                    "recharge core": 69,
                },
            },
        }
    },
}

TESLA_MODEL_MAPPING = {
    "3": "model 3",
    "Y": "model y",
    "X": "model x",
    "S": "model s",
    "T": "tesla semi",
    "R": "roadster",
    "C": "cybertruck",
}


def mapping_vehicle_type(
    type_car, make_name, model_name, db_df, battery_capacity=None, sale_year=None
):
    """Map a given vehicle to the closest model identifier in the database.
    Args:
        type_car (str): type car to find match
        make_name (str): make car
        model_name (str): model car
        db_df (pd.DataFrame): db with all the model in dbeaver
        battery_capacity (str, optional): capacity car battery. Defaults to None.

    Returns:
        str: type le plus proche présent dans la db de vehicle_model
    """
    if not model_name or model_name == "unknown":
        raise ValueError("missing model name")
    if battery_capacity:
        try:
            db_df["capacity"] = db_df["capacity"].astype(float)
        except:
            print(
                "column battery capacity doesn't have the right name please change to capacity"
            )
    make_name = make_name.lower().replace("ë", "e")
    type_car = type_car.lower()

    ## special case for zoe
    if model_name == "zoe":
        pattern_1 = r"(?i)((r|q)\d+)"
        pattern_2 = r"(?<!\d)(50|40)(?!\d)"
        type_car_1 = re.search(pattern_1, type_car)
        type_car_2 = re.search(pattern_2, type_car)
        if type_car_1:
            type_car_1 = type_car_1.group(1)
        if type_car_2:
            type_car_2 = type_car_2.group(1)
        if type_car_1 is not None:
            type_car = (str(type_car_1) + " ze" + str(type_car_2)).replace(
                " zeNone", ""
            )
    ## special case for scenic
    if "scenic" in model_name:
        model_name = "scenic"

    try:
        d = re.findall(r"\d*", model_name)
        d.sort()
        nbr = d[-1]
        model_name = nbr or model_name.lower()
    except:
        model_name = model_name.lower()
    # filter on OEM
    if make_name in ["mercedes", "mercedes-benz"]:
        subset = db_df[db_df["make_name"].isin(["mercedes", "mercedes-benz"])].copy()
    else:
        subset = db_df[db_df["make_name"] == make_name].copy()
    # Find the best match
    # Returns the closest model, score_cutoff set to 0.1 for now to ensure we almost always get a result
    match_model = process.extractOne(
        model_name, subset["model_name"], scorer=fuzz.token_sort_ratio, score_cutoff=0.1
    )
    if match_model:
        match_model_name, _, _ = match_model
        # filter on model name
        subset = subset[subset["model_name"] == match_model_name]
        # find the battery with the closest capacity
        try:
            if battery_capacity:
                battery_target = float(
                    battery_capacity.lower().replace("kwh", "").strip()
                )
                subset["distance"] = (subset["capacity"] - battery_target).abs()
                closest_rows = subset[subset["distance"] == subset["distance"].min()]

            elif sale_year:
                sale_year = pd.to_datetime(sale_year)
                today = pd.to_datetime(date.today())
                subset[["commissioning_date", "enf_of_life_date"]].fillna(today)
                subset = subset[
                    (subset["commissioning_date"] <= sale_year)
                    & (subset["enf_of_life_date"] >= sale_year)
                ]
                closest_rows = subset
            else:
                closest_rows = subset
            # match on type
            match_type = process.extractOne(
                type_car, closest_rows["type"], scorer=fuzz.token_sort_ratio
            )
            if match_type:
                _, _, index = match_type
                return closest_rows.loc[index]["id"]

        # fallback: find the closest type without battery
        except:
            match_type = process.extractOne(
                type_car, subset["type"], scorer=fuzz.token_sort_ratio
            )
            _, _, index = match_type
            return subset.loc[index, "id"]

    return "unknown"
