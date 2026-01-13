"""Client for interacting with Gotenberg PDF generation service."""

import logging
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)


class GotenbergClient:
    """Client for generating PDFs using Gotenberg service."""

    def __init__(self, base_url: str):
        """
        Initialize Gotenberg client.

        Args:
            base_url: Base URL of the Gotenberg service
        """
        self.base_url = base_url.rstrip("/")
        self.chromium_endpoint = f"{self.base_url}/forms/chromium/convert/html"

    async def generate_pdf_from_html(
        self,
        html_content: str,
        wait_delay: str | None = "2s",
        paper_width: float = 8.27,  # A4 width in inches
        paper_height: float = 11.69,  # A4 height in inches
        margin_top: float = 0,
        margin_bottom: float = 0,
        margin_left: float = 0,
        margin_right: float = 0,
        print_background: bool = True,
        prefer_css_page_size: bool = False,
        scale: float | None = None,
        additional_files: dict[str, bytes] | None = None,
    ) -> bytes:
        """
        Generate PDF from HTML content using Gotenberg.

        Args:
            html_content: HTML content as string
            wait_delay: Time to wait before generating PDF (e.g., "2s", "500ms")
            paper_width: Paper width in inches
            paper_height: Paper height in inches
            margin_top: Top margin in inches
            margin_bottom: Bottom margin in inches
            margin_left: Left margin in inches
            margin_right: Right margin in inches
            print_background: Whether to print background graphics
            prefer_css_page_size: Use CSS page size instead of paper dimensions
            scale: Page scale factor (1.0 = 100%, e.g., 1.5 = 150%)
            additional_files: Additional files to send (e.g., CSS, images) as {filename: bytes}

        Returns:
            PDF content as bytes
        """
        files = {
            "index.html": ("index.html", html_content.encode("utf-8"), "text/html"),
        }

        # Add any additional files (CSS, images, etc.)
        if additional_files:
            for filename, content in additional_files.items():
                mime_type = self._get_mime_type(filename)
                files[filename] = (filename, content, mime_type)

        # Form data for Gotenberg options
        data = {
            "paperWidth": str(paper_width),
            "paperHeight": str(paper_height),
            "marginTop": str(margin_top),
            "marginBottom": str(margin_bottom),
            "marginLeft": str(margin_left),
            "marginRight": str(margin_right),
            "printBackground": str(print_background).lower(),
            "preferCssPageSize": str(prefer_css_page_size).lower(),
        }

        if wait_delay:
            data["waitDelay"] = wait_delay

        if scale is not None:
            data["scale"] = str(scale)

        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                self.chromium_endpoint,
                files=files,
                data=data,
            )
            if response.status_code != 200:
                raise Exception(
                    f"Failed to generate PDF on {self.chromium_endpoint}: {response.text}"
                )
            return response.content

    @staticmethod
    def _get_mime_type(filename: str) -> str:
        """Get MIME type based on file extension."""
        ext = Path(filename).suffix.lower()
        mime_types = {
            ".html": "text/html",
            ".css": "text/css",
            ".js": "application/javascript",
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".svg": "image/svg+xml",
            ".gif": "image/gif",
            ".webp": "image/webp",
        }
        return mime_types.get(ext, "application/octet-stream")
