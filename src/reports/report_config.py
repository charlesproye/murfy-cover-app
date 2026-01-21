import os

FRONTEND_URL = os.environ["FRONTEND_URL"].rstrip("/")
VERIFY_REPORT_BASE_URL = FRONTEND_URL + "/verify-report/"

GOTENBERG_URL = os.getenv("GOTENBERG_URL", "http://localhost:3030")
