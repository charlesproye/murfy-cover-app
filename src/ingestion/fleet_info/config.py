

ACCOUNT_TOKEN_KEYS = {
    'OLINO': 'ACCESS_TOKEN_OLINO',
    'AYVENS_SLBV': 'ACCESS_TOKEN_AYVENS_SLBV',
    'AYVENS_BLBV': 'ACCESS_TOKEN_AYVENS_BLBV',
    'AYVENS': 'ACCESS_TOKEN_AYVENS',
    'AYVENS_NV': 'ACCESS_TOKEN_AYVENS_NV',
    'AYVENS_NVA': 'ACCESS_TOKEN_AYVENS_NVA',
    'CAPFM': 'ACCESS_TOKEN_CAPFM'
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

