import uuid

from reports.report_config import VERIFY_REPORT_BASE_URL
from reports.reports_utils import generate_report_qr_code_data_url


def test_generate_report_qr_code() -> None:
    report_id = uuid.uuid4()
    qr_code = generate_report_qr_code_data_url(f"{VERIFY_REPORT_BASE_URL}/{report_id}")
    assert qr_code is not None
