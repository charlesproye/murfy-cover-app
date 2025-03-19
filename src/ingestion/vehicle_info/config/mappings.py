MAKE_MAPPING = {
    'mercedes-benz': 'mercedes',
    'mercedes': 'mercedes',
    'Mercedes': 'mercedes',
    'Mercedes-Benz': 'mercedes',
    'MERCEDES': 'mercedes',
    'MERCEDES-BENZ': 'mercedes',
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
    #other
    "TESLA": "tesla",
    "Tesla": "tesla",
    "Renault": "renault",
    "RENAULT": "renault",
}

OEM_MAPPING = {
    'Mercedes': 'mercedes',
    'MERCEDES': 'mercedes',
    'mercedes-benz': 'mercedes',
    'Mercedes-Benz': 'mercedes',
    'MERCEDES-BENZ': 'mercedes',
    'Mercedes': 'mercedes',
    'Stellantis': 'stellantis',
    'STELLANTIS': 'stellantis',
    'stellantis': 'stellantis'
}

COUNTRY_MAPPING = {
    'nl': 'Netherlands',
    'fr': 'France',
    'be': 'Belgium',
    'de': 'Germany',
    'lu': 'Luxembourg',
    'es': 'Spain',
    'it': 'Italy',
    'pt': 'Portugal',
    'gb': 'United Kingdom',
    'uk': 'United Kingdom'
} 

COL_DTYPES = {
    'vin': str,
    'licence_plate': str,
    'make': str,
    'model': str,
    'type': str,
    'oem': str,
    'owner': str,
    'country': str,
    'activation': bool,
    'EValue': bool,
    'eligibility': bool,
    'real_activation': bool,
    'activation_error':str
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
