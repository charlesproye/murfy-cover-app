"""Utilities for handling SVG and PNG assets in PDF reports."""

import base64
from pathlib import Path


class AssetEmbedder:
    """Handles embedding of SVG and PNG assets for PDF generation."""

    def __init__(self, assets_dir: Path):
        """
        Initialize the asset embedder.

        Args:
            assets_dir: Path to the directory containing assets (svg/ and images/ subdirectories)
        """
        self.assets_dir = Path(assets_dir)
        self.svg_dir = self.assets_dir / "svg"
        self.images_dir = self.assets_dir / "images"

    def get_svg_inline(self, filename: str) -> str:
        """
        Get SVG content as inline string.

        Args:
            filename: Name of the SVG file (e.g., 'logo.svg')

        Returns:
            SVG content as string
        """
        svg_path = self.svg_dir / filename
        if not svg_path.exists():
            raise FileNotFoundError(f"SVG file not found: {svg_path}")
        return svg_path.read_text(encoding="utf-8")

    def get_svg_data_url(self, filename: str) -> str:
        """
        Get SVG as data URL for use in CSS or img src.

        Args:
            filename: Name of the SVG file

        Returns:
            Data URL string (data:image/svg+xml;base64,...)
        """
        svg_content = self.get_svg_inline(filename)
        b64_content = base64.b64encode(svg_content.encode("utf-8")).decode("utf-8")
        return f"data:image/svg+xml;base64,{b64_content}"

    def get_png_data_url(self, filename: str) -> str:
        """
        Get PNG as base64 data URL.

        Args:
            filename: Name of the PNG file

        Returns:
            Data URL string (data:image/png;base64,...)
        """
        png_path = self.images_dir / filename
        if not png_path.exists():
            raise FileNotFoundError(f"PNG file not found: {png_path}")

        png_bytes = png_path.read_bytes()
        b64_content = base64.b64encode(png_bytes).decode("utf-8")
        return f"data:image/png;base64,{b64_content}"

    def get_all_assets_as_data_urls(self) -> dict[str, str]:
        """
        Get all assets as data URLs for template rendering.

        Returns:
            Dictionary mapping asset names to data URLs
        """
        assets = {}

        # Process all SVG files
        if self.svg_dir.exists():
            for svg_file in self.svg_dir.glob("*.svg"):
                key = f"svg_{svg_file.stem}"
                assets[key] = self.get_svg_data_url(svg_file.name)

        # Process all PNG files
        if self.images_dir.exists():
            for png_file in self.images_dir.glob("*.png"):
                key = f"img_{png_file.stem}"
                assets[key] = self.get_png_data_url(png_file.name)

        return assets
