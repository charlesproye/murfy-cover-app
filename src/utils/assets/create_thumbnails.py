#!/usr/bin/env python3
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

THUMB_SIZE = 256
THUMB_WEBP_QUALITY = 80
THUMB_WEBP_METHOD = 6
S3_BUCKET = "bib-batteries-assets"
CACHE_CONTROL = "public, max-age=31536000, immutable"
SRC_DIR = Path("/tmp/bib-assets/")


def check_root_folders() -> None:
    allowed_folders = {"car_images", "make_images"}

    for f in SRC_DIR.glob("*"):
        if not f.is_dir():
            raise ValueError(
                f"Root folder {f.name} is not a directory, {SRC_DIR} must only contain {allowed_folders}"
            )
        if f.name not in allowed_folders:
            raise ValueError(
                f"Root folder {f.name} is not allowed, {SRC_DIR} must only contain {allowed_folders}"
            )


def is_thumbnail(path: Path) -> bool:
    return bool(re.search(r"_\d+px\.(webp|jpg|jpeg)$", path.name, re.IGNORECASE))


def validate_filename(path: Path) -> tuple[bool, str]:
    """
    Validate that filename is normalized, lowercase, starts with OEM name, and has no special characters.

    Args:
        path: Path to the file to validate (must be relative to SRC_DIR)

    Returns:
        Tuple of (is_valid, error_message)
    """
    filename = path.name
    parent_name = path.parent.name

    # Check if filename is lowercase
    if filename != filename.lower():
        return False, f"Filename must be lowercase: {filename}"

    # Check if filename starts with parent folder name (OEM name)
    # Remove extension for comparison
    name_without_ext = path.stem
    expected_prefix = parent_name.lower()
    if not name_without_ext.startswith(expected_prefix + "_"):
        return False, f"Filename must start with OEM name '{parent_name}_': {filename}"

    # Check for special characters (allow only alphanumeric, underscore, hyphen, and dot for extension)
    # Pattern: OEM_name_rest_of_name.ext
    allowed_pattern = re.compile(r"^[a-z0-9_-]+\.[a-z]+$")
    if not allowed_pattern.match(filename):
        return (
            False,
            f"Filename contains invalid characters (only lowercase letters, numbers, underscores, hyphens allowed): {filename}",
        )

    # Check for spaces
    if " " in filename:
        return False, f"Filename must not contain spaces: {filename}"

    return True, ""


def to_jpg_from_webp(src: Path) -> Path | None:
    dst = src.with_suffix(".jpg")
    if dst.exists():
        return None
    subprocess.run(
        [
            "magick",
            str(src),
            "-colorspace",
            "sRGB",
            "-strip",
            "-quality",
            "100",
            str(dst),
        ],
        check=True,
    )
    return dst


def to_webp_from_jpg(src: Path) -> Path | None:
    dst = src.with_suffix(".webp")
    if dst.exists():
        return None
    subprocess.run(
        [
            "magick",
            str(src),
            "-colorspace",
            "sRGB",
            "-strip",
            "-define",
            "webp:lossless=true",
            str(dst),
        ],
        check=True,
    )
    return dst


def thumb_from_webp(src: Path) -> Path | None:
    # Resize largest dimension to THUMB_SIZE, keeping aspect ratio
    dst = src.parent / f"{src.stem}_{THUMB_SIZE}px.webp"
    if dst.exists():
        return None
    subprocess.run(
        [
            "magick",
            str(src),
            "-resize",
            f"{THUMB_SIZE}x{THUMB_SIZE}>",
            "-colorspace",
            "sRGB",
            "-strip",
            "-define",
            f"webp:method={THUMB_WEBP_METHOD}",
            "-quality",
            str(THUMB_WEBP_QUALITY),
            str(dst),
        ],
        check=True,
    )
    return dst


def main() -> None:
    try:
        check_root_folders()
    except ValueError as e:
        print(f"❌ {e}")
        return

    created_files = []
    original_files = []
    invalid_files = []

    print(f"Collecting existing image files from {SRC_DIR}...")
    # Collect existing image files and validate filenames
    for f in SRC_DIR.rglob("*"):
        if f.is_file() and f.suffix.lower() in (".jpg", ".jpeg", ".webp"):
            # Skip thumbnails from validation (they're auto-generated)
            if not is_thumbnail(f):
                is_valid, error_msg = validate_filename(f)
                if not is_valid:
                    invalid_files.append((f, error_msg))
                    continue
            original_files.append(f)

    # Pass 1: ensure full-size counterparts exist
    for f in SRC_DIR.rglob("*"):
        if not f.is_file():
            continue
        if is_thumbnail(f):
            continue

        ext = f.suffix.lower()
        if ext == ".webp":
            created = to_jpg_from_webp(f)
            if created:
                created_files.append(created)
        elif ext in (".jpg", ".jpeg"):
            created = to_webp_from_jpg(f)
            if created:
                created_files.append(created)

    # Pass 2: generate thumbnails from full-size WebP
    for f in SRC_DIR.rglob("*.webp"):
        if is_thumbnail(f):
            continue
        created = thumb_from_webp(f)
        if created:
            created_files.append(created)

    # Report validation errors
    if invalid_files:
        print("\n❌ Invalid filenames found (will be skipped):")
        for f, error_msg in invalid_files:
            print(f"    • {f.relative_to(SRC_DIR)}")
            print(f"      Error: {error_msg}")
        print(f"\n  Total invalid files: {len(invalid_files)}")
        return

    # Confirmation prompt
    print("\n📦 Files to upload to S3:")
    print(f"\n  Original images: {len(original_files)} files")
    for f in sorted(original_files)[:5]:
        print(f"    • {f.relative_to(SRC_DIR)}")
    if len(original_files) > 5:
        print(f"    ... and {len(original_files) - 5} more")

    print(f"\n  Converted/thumbnails: {len(created_files)} files")
    for f in sorted(created_files)[:5]:
        print(f"    • {f.relative_to(SRC_DIR)}")
    if len(created_files) > 5:
        print(f"    ... and {len(created_files) - 5} more")

    print(
        f"\n  Total: {len(original_files) + len(created_files)} files -> s3://{S3_BUCKET}/"
    )

    response = input("\n🚀 Proceed with upload? [y/N]: ").strip().lower()
    if response not in ("y", "yes"):
        print("❌ Upload cancelled")
        return

    # Pass 3: sync everything to S3
    subprocess.run(
        [
            "aws",
            "s3",
            "sync",
            SRC_DIR,
            f"s3://{S3_BUCKET}/",
        ],
        check=True,
    )

    # Pass 4: make only thumbnails public-read (parallelized with boto3)
    print("\n🔓 Setting thumbnails to public-read...")
    thumbnail_keys = [
        str(f.relative_to(SRC_DIR))
        for f in SRC_DIR.rglob(f"*_{THUMB_SIZE}px.webp")
        if ".git" not in f.parts
    ]

    if not thumbnail_keys:
        print("  No thumbnails found to update")
    else:
        s3_client = boto3.client("s3")

        def update_thumbnail_metadata(key: str) -> None:
            """Update ACL, cache-control, and content-type for a thumbnail."""
            try:
                # Use copy_object with MetadataDirective='REPLACE' to update metadata
                # This is faster than copying via subprocess and allows metadata updates
                s3_client.copy_object(
                    Bucket=S3_BUCKET,
                    Key=key,
                    CopySource={"Bucket": S3_BUCKET, "Key": key},
                    ACL="public-read",
                    CacheControl=CACHE_CONTROL,
                    ContentType="image/webp",
                    MetadataDirective="REPLACE",
                )
            except ClientError as e:
                print(f"  ⚠️  Failed to update {key}: {e}")

        # Process in parallel (max 50 concurrent operations)
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = {
                executor.submit(update_thumbnail_metadata, key): key
                for key in thumbnail_keys
            }
            for i, future in enumerate(as_completed(futures)):
                if i % 100 == 0:
                    print(f"  Updated {i}/{len(thumbnail_keys)} thumbnails...")
                future.result()  # Raise any exceptions
        print(f"  ✔ Updated {len(thumbnail_keys)} thumbnails")

    print(
        f"✔ Done: synced all files to s3://{S3_BUCKET}/ and set public-read ACL only for *_{THUMB_SIZE}px.webp."
    )


if __name__ == "__main__":
    main()
