# DB
RIGHT_MERGES_RDB_TABLES_FLEET_INFO_DEST_COLS = [
    "fleet_id",
    "region_id",
    "vehicle_model_id",
    "licence_plate",
    "end_of_contract",
    "id",
    "activation_status",
    "vin",
]
LEFT_MERGES_RDB_TABLES_FLEET_INFO_KWARGS = [
    dict(rhs="vehicle_model",left_on=["model", "version"], right_on=["model_name", "type"], src_dest_cols={"id": "vehicle_model_id"}),
    dict(rhs="region", left_on="country", right_on="region_name", src_dest_cols={"id": "region_id"}),
    dict(rhs="fleet", left_on="fleet", right_on="fleet_name", src_dest_cols={"id": "fleet_id"}),
]
COLS_TO_LOAD_IN_RDB_VEHICLE_TABLE = [
    "fleet_id", #
    "region_id", #
    "vehicle_model_id", #
    "purchase_date",
    "licence_plate", #
    "end_of_contract", #
    "id",
    #"updated_at",
    #"created_at",
    "activation_status", #
    "vin", #
]

#S3
# TEST_TESLA_FLEET_INFO_KEY = "fleet_info/tesla/raw_fleet_info.json"
# S3_JSON_FLEET_INFO_RESPONSE_KEYS = [
#     "fleet_info/tesla/raw_fleet_info.json",
#     "fleet_info/tesla/ayvens-blbv.json",
#     "fleet_info/tesla/ayvens-nv.json",
#     "fleet_info/tesla/ayvens-nva.json",
#     "fleet_info/tesla/ayvens-slbv.json",
#     "fleet_info/tesla/ayvens.json",
# ]
# All
FLEET_INFO_KEY = "fleet_info/fleet_info.csv"

# Ayvens
# AYVENS_FLEET_INFO_CSV_KEY = "fleet_info/ayvens/fleet_info_with_regions.csv"
# AYVENS_FLEET_WITH_ONLY_CONTRACT_START_DATE_KEY = "fleet_info/ayvens/fleet_info - Global NL.csv"
# AYVENS_FLEET_WITH_CAR_REGISTRATION_KEY = "fleet_info/ayvens/fleet_info - Global NL 2.csv"
# AYVENS_FLEET_INFO_PARQUET_KEY = "fleet_info/ayvens/fleet_info.parquet"

FLEET_INFO_COLS_NAME_MAPPING = {
    "End of Contract": "end_of_contract",
    "Activation": "activation_status",
    "Type": "version",
    "Ownership ": "owner",
    "Make": "make",
    "VIN": "vin",
    "Licence plate": "licence_plate",
    "Country": "country",
    "Model": "model"
}
MODEL_VERSION_NAME_MAPPING = {
    "R90 Life (batterijkoop) 5d": "R90",
    "R135 Edition One (batterijkoop) 5d": "R135",
    "R135 Intens (batterijkoop) 5d": "R135",
    "R135": "R135",
}
MAKE_NAME_MAPPING = {
    # bmw
    "BMW": "bmw",
    # high mobility
    "FORD": "ford",
    "Mercedes-Benz": "mercedes-benz",
    "MERCEDES-BENZ": "mercedes-benz",
    "Mercedes": "mercedes-benz",
    "KIA": "kia",
    "Kia": "kia",
    # stellantis
    "PEUGEOT": "peugeot",
    "Peugeot": "peugeot",
    "OPEL": "opel",
    "Opel": "opel",
    "FIAT": "fiat",
    "Fiat": "fiat",
    "Renault": "renault",
    "RENAULT": "renault",
    "TESLA": "tesla",
    "Tesla": "tesla",
    "Citroën": "citroën",
    "DS": "ds",
    # Add more variations
    "tesla": "tesla",  # Add lowercase version
    "TESLA": "tesla"   # Ensure uppercase version maps to lowercase
}
COUNTRY_NAME_MAPPING = {
    "NL": "netherlands"   
}
COL_DTYPES = {
    "licence_plate": "string",
    "make": "string",
    "model": "string",
    "version": "string",
    # "capacity": "float",
    # "autonomie": "float",
    "end_of_contract": "datetime64[ns]",
    "end_of_contract_date": "datetime64[ns]",
    "country": "string",
    "category": "string",
    "activation_status": "string",
    "owner": "string",
    # "registration_date": "object",
    "contract_start_date": "datetime64[ns]",
    "vin": "string",
}
suffixes_to_remove = ['5d', '4d', '3d', 'auto', 'aut', 'actieauto', 'onze deal', 'business', 'executive', 'edition', 'line', 'r-design']
mappings = {
        'bmw': {
            'i3': {  # Un seul modèle i3
                'patterns': [
                    (r'.*i3s.*120\s*ah.*', 'i3s 120ah'),  # i3s doit être avant i3 pour être vérifié en premier
                    (r'.*120\s*ah.*', '120ah'),           # i3 120ah standards
                    (r'.*94\s*ah.*|.*92\s*ah.*', '94ah'), # Capture les deux variantes 94/92ah
                ],
                'model_clean': lambda m: 'i3'
            },
            'i4': {
                'patterns': [(r'.*', 'x')],
                'model_clean': lambda m: 'i4'
            }
        },
        'ds': {
            'ds 3 crossback': {
                'patterns': [(r'.*e-tense 50\s*kwh.*', 'e-tense 50 kwh')],
            },
            'ds 7 crossback': {
                'patterns': [(r'.*e-tense 4x4.*', 'e-tense 4x4')],
            },
        },
        'mercedes': {
            'eqa': {
                'patterns': [(r'.*250.*|.*eqa.*', '250')],
                'model_clean': lambda m: 'eqa'
            },
            'eqb': {
                'patterns': [(r'.*250.*|.*eqb.*', '250')],
                'model_clean': lambda m: 'eqb'
            },
            'eqc': {
                # Tous les EQC sont des 400 4matic, peu importe les suffixes (amg, solution, luxury, etc.)
                'patterns': [(r'.*400.*4matic.*', '400 4matic')],
                'model_clean': lambda m: 'eqc'
            },
            'eqs': {
                # Tous les EQS sont des 450+, peu importe les suffixes (luxury, etc.)
                'patterns': [(r'.*450\+.*', '450+')],
                'model_clean': lambda m: 'eqs'
            },
            'sprinter': {
                'patterns': [(r'.*47kwh.*|.*sprinter.*', '47kwh')],
                'model_clean': lambda m: 'sprinter'
            },
            'vito': {
                'patterns': [(r'.*35kwh.*|.*vito.*', '35kwh')],
                'model_clean': lambda m: 'vito'
            },
        },
        'kia': {
            'e-niro': {
                'patterns': [(r'.*64kwh.*|.*x.*', '64kwh')],
            },
            'ev6': {
                'patterns': [(r'.*77\.4kwh.*rwd.*|.*x.*', '77.4kwh rwd')],
            },
        },
        'peugeot': {
            '208': {
                'patterns': [(r'.*50kwh 136.*', 'ev 50kwh 136')],
            },
            '2008': {
                'patterns': [(r'.*50kwh 136.*', 'ev 50kwh 136')],
                'model_clean': lambda m: '2008'  # Unifie e-2008 et 2008
            },
            'e-2008': {
                'patterns': [(r'.*50kwh 136.*', 'ev 50kwh 136')],
                'model_clean': lambda m: '2008'  # Unifie e-2008 et 2008
            },
        },
        'renault': {
            'zoe': {
                'patterns': [
                    (r'.*r110.*', 'R110'),
                    (r'.*r135.*', 'R135'),
                ],
                'model_clean': lambda m: 'zoe',
                'metadata': {
                    'url_image': {
                        'R110': 'https://olinn.eu/sites/default/files/styles/max_650/public/images/zoe-1-2.png?itok=WtFDoX9b',
                        'R135': 'https://carvo.ch/assets/images/models/md-5/medium/renault-zoe.png'
                    },
                    'warranty_km': 160000,
                    'warranty_date': {
                        'R110': 8,
                        'R135': 6
                    },
                    'capacity': {
                        'R110': 52,
                        'R135': 52
                    }
                }
            }
        },
        'ford': {
            'mustang mach-e': {
                'patterns': [
                    # 75 kWh versions
                    (r'.*75kwh.*awd.*(?:tech.*pack.*)?', '75kwh awd'),     # Ignore tech pack/plus
                    (r'.*75kwh.*rwd.*(?:tech.*pack.*)?', '75kwh rwd'),     # Ignore tech pack/plus
                    # 98 kWh versions
                    (r'.*98kwh.*extended.*range.*awd.*(?:tech.*pack.*)?', '98kwh extended range awd'),
                    (r'.*98kwh.*extended.*range.*rwd.*(?:tech.*pack.*)?', '98kwh extended range rwd'),
                ],
                'model_clean': lambda m: 'mustang mach-e'
            },
            'e-transit': {
                'patterns': [(r'.*', 'x')],
                'model_clean': lambda m: 'e-transit'
            }
        },
        'volvo': {
            'xc40': {
                'patterns': [
                    (r'.*p8.*awd.*', 'p8 awd'),
                    (r'.*recharge.*twin.*', 'recharge twin'),
                    (r'.*recharge.*plus.*', 'recharge plus'),
                    (r'.*recharge.*core.*', 'recharge core')
                ],
                'model_clean': lambda m: 'xc40',
                'metadata': {
                    'url_image': 'https://cas.volvocars.com/image/dynamic/MY25_2417/536/exterior-v1/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/default.png?market=fr&client=ccs-self-service&fallback&angle=4&w=750&bg=00000000',
                    'warranty_km': 160000,
                    'warranty_date': 8,
                    'capacity': {
                        'p8 awd': 75,
                        'recharge twin': 78,
                        'recharge plus': 69,
                        'recharge core': 69
                    }
                }
            }
        }
    }
ACCOUNT_TOKEN_KEYS = {
    'OLINO': 'ACCESS_TOKEN_OLINO',
    'AYVENS_SLBV': 'ACCESS_TOKEN_AYVENS_SLBV',
    'AYVENS_BLBV': 'ACCESS_TOKEN_AYVENS_BLBV',
    'AYVENS': 'ACCESS_TOKEN_AYVENS',
    'AYVENS_NV': 'ACCESS_TOKEN_AYVENS_NV',
    'AYVENS_NVA': 'ACCESS_TOKEN_AYVENS_NVA'
}

RATE_LIMIT_DELAY = 0.5  
MAX_RETRIES = 3 

TESLA_PATTERNS = {
    'model 3': {
        'patterns': [
            (r'.*standard range.*plus.*rear.?wheel.*|.*standard range.*plus.*rwd.*|.*rear.?wheel drive.*', 'RWD'),
            (r'.*performance.*dual motor.*|.*performance.*', 'Performance'),
            (r'.*long range.*all.?wheel drive.*', 'Long Range AWD'),
        ]
    },
    'model s': {
        'patterns': [
            (r'.*100d.*', '100D'),
            (r'.*75d.*', '75D'),
            (r'.*long range.*plus.*', 'Long Range Plus'),
            (r'.*long range.*', 'Long Range'),
            (r'.*plaid.*', 'Plaid'),
            (r'.*performance.*', 'Performance'),
            (r'.*standard range.*', 'Standard Range'),
        ]
    },
    'model x': {
        'patterns': [
            (r'.*long range.*plus.*', 'Long Range Plus'),
            (r'.*long range.*', 'Long Range'),
        ]
    },
    'model y': {
        'patterns': [
            (r'.*rear.?wheel drive.*', 'RWD'),
            (r'.*long range.*rwd.*', 'Long Range RWD'),
            (r'.*long range.*all.?wheel drive.*', 'Long Range AWD'),
            (r'.*performance.*awd.*', 'Performance'),
        ]
    }
}
