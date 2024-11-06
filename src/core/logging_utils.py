from rich.logging import RichHandler
import logging.config

RICH_HANDLER = RichHandler()
FORMAT = "%(message)s"
logging.basicConfig(
    level="WARNING", format=FORMAT, datefmt="[%X]", handlers=[RICH_HANDLER]
)

def set_level_of_loggers_with_prefix(level, logger_name_prefix:str):
    logging.config.dictConfig({
        'version': 1,
        "loggers": {
            logger_name_prefix: dict(
                level=level,
                datefmt="[%X]",
                handlers=["rich"],
                propagate=False,
            ),
        },
        'handlers': {
            'rich': {
                'class': ['rich.logging.RichHandler'],
                'formatter': 'rich',
            },
        },
        'formatters': {
            'rich': {
                'format': FORMAT,
            },
        },
    })
