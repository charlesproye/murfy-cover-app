import logging.config


def set_level_of_loggers_with_prefix(level, logger_name_prefix:str):
    logging.config.dictConfig({
        'version': 1,
        "loggers": {
            logger_name_prefix: {
                "level": level,
                'handlers': ['console'],
                'propagate': False,
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'default',
            },
        },
        'formatters': {
            'default': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            },
        },
    })

