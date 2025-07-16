import logging
import logging.config
from typing import Annotated
from fastapi import Depends
from rich.logging import RichHandler

RICH_HANDLER = RichHandler()
FORMAT = "%(message)s"

def set_level_of_loggers_with_prefix(level, logger_name_prefix: str):
    if not isinstance(logger_name_prefix, str):
        raise TypeError("Logger name prefix must be a string")

    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'loggers': {
            logger_name_prefix: {
                'level': level,
                'handlers': ['rich'],
                'propagate': False,
            },
        },
        'handlers': {
            'rich': {
                'class': 'rich.logging.RichHandler',
                'formatter': 'rich',
            },
        },
        'formatters': {
            'rich': {
                'format': FORMAT,
            },
        },
    })
