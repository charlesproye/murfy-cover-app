import os


bucket = os.getenv("S3_BUCKET", "")

blue = "\x1b[38;5;27m"
green = "\x1b[38;5;46m"
reset = "\x1b[0m"


bucket_dev = 'bib-platform-dev-data'
bucket_prod = 'bib-platform-prod-data'

if bucket == bucket_dev:
    PLATFORM_COLORED = green + "dev" + reset
    PLATFORM = "dev"
elif bucket == bucket_prod:
    PLATFORM_COLORED = blue + "prod" + reset
    PLATFORM = "prod"
else:
    raise ValueError(f"Unknown bucket: {bucket}")




