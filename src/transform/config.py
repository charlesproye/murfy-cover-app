MAIN_KWARGS = {
    "--log-level": {
        "default": "INFO",
        "type": str,
        "help": "Set the logging level (e.g., DEBUG, INFO)",
    },
    "--pipelines": {
        "required": False,
        "help": "Specifies pipeline or list of pipelines to run."
    },
    "--steps": {
        "required": False,
        "help": "Specifies pipeline step or list of pipelines steps to run."
    },
}

