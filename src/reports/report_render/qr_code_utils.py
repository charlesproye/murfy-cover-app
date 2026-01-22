import base64
import io

import qrcode
from qrcode.image.styledpil import StyledPilImage
from qrcode.image.styles.colormasks import (
    SquareGradiantColorMask,
)
from qrcode.image.styles.moduledrawers.pil import RoundedModuleDrawer


def generate_report_qr_code_data_url(report_url: str) -> str:
    """
    Generate a QR code as a base64 PNG data URL (in-memory, no disk I/O).

    Args:
        report_url: URL of the report

    Returns:
        Base64 data URL string ready for <img src="...">
    """
    qr = qrcode.QRCode(
        version=1,
        box_size=6,
        border=2,
        image_factory=StyledPilImage,
        error_correction=qrcode.ERROR_CORRECT_Q,
    )
    qr.add_data(report_url)
    qr.make(fit=True)

    img = qr.make_image(
        module_drawer=RoundedModuleDrawer(),
        color_mask=SquareGradiantColorMask(
            back_color=(204, 237, 220),
            center_color=(0, 220, 110),
            edge_color=(5, 68, 43),
        ),
    )

    buffer = io.BytesIO()
    img.save(buffer, format="PNG")
    buffer.seek(0)

    base64_str = base64.b64encode(buffer.getvalue()).decode("utf-8")
    return f"data:image/png;base64,{base64_str}"
