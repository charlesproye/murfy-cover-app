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
    'NL': 'Netherlands',
    'FR': 'France',
    'BE': 'Belgium',
    'DE': 'Germany',
    'LU': 'Luxembourg',
    'ES': 'Spain',
    'IT': 'Italy',
    'PT': 'Portugal',
    'GB': 'United Kingdom',
    'UK': 'United Kingdom'
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
    'real_activation': bool
}
