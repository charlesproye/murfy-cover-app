import os

from dotenv import load_dotenv


load_dotenv(override=True)

def get_env_var(var_name: str) -> str:
    assert var_name in os.environ, f"{var_name} is not in the environment."
    return os.getenv(var_name)

