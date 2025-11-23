#!/usr/bin/env python3
"""
Download FEC bulk data files.

Downloads and extracts bulk data files from FEC website for a specific election cycle.

Usage:
    # Download all files for 2026 cycle
    poetry run python scripts/download_bulk_data.py --cycle 2026 --output-dir data/2025-2026

    # Download only specific file types
    poetry run python scripts/download_bulk_data.py --cycle 2026 --output-dir data/2025-2026 --files cm cn indiv

    # Skip extraction (keep .zip files)
    poetry run python scripts/download_bulk_data.py --cycle 2026 --output-dir data/2025-2026 --no-extract
"""

import argparse
import logging
import sys
import zipfile
from datetime import datetime
from pathlib import Path
from typing import List
from urllib.parse import urljoin

import requests

# Base URL for FEC bulk data
FEC_BULK_DATA_URL = "https://www.fec.gov/files/bulk-downloads/"

# Available bulk data files and their descriptions
BULK_FILES = {
    "cm": {
        "description": "Committee Master File",
        "zip_name": "cm{yy}.zip",
        "contains": ["cm.txt"],
        "header_url": "https://www.fec.gov/files/bulk-downloads/data_dictionaries/cm_header_file.csv",
    },
    "cn": {
        "description": "Candidate Master File",
        "zip_name": "cn{yy}.zip",
        "contains": ["cn.txt"],
        "header_url": "https://www.fec.gov/files/bulk-downloads/data_dictionaries/cn_header_file.csv",
    },
    "ccl": {
        "description": "Candidate-Committee Linkages",
        "zip_name": "ccl{yy}.zip",
        "contains": ["ccl.txt"],
        "header_url": "https://www.fec.gov/files/bulk-downloads/data_dictionaries/ccl_header_file.csv",
    },
    "indiv": {
        "description": "Individual Contributions",
        "zip_name": "indiv{yy}.zip",
        "contains": ["itcont.txt", "by_date/"],  # Contains subdirectory
        "header_url": "https://www.fec.gov/files/bulk-downloads/data_dictionaries/indiv_header_file.csv",
        "large": True,  # Flag for large files
        "extract_to_subdir": "indiv{yy}",  # Extract to subdirectory
    },
    "pas2": {
        "description": "Committee-to-Candidate Contributions",
        "zip_name": "pas2{yy}.zip",
        "contains": ["itpas2.txt"],
        "header_url": "https://www.fec.gov/files/bulk-downloads/data_dictionaries/pas2_header_file.csv",
    },
    "oth": {
        "description": "Committee-to-Committee Transactions",
        "zip_name": "oth{yy}.zip",
        "contains": ["itoth.txt"],
        "header_url": "https://www.fec.gov/files/bulk-downloads/data_dictionaries/oth_header_file.csv",
    },
    "oppexp": {
        "description": "Operating Expenditures",
        "zip_name": "oppexp_{cycle}.zip",
        "contains": ["oppexp.txt"],
        "header_url": "https://www.fec.gov/files/bulk-downloads/data_dictionaries/oppexp_header_file.csv",
    },
}

# Files needed for basic ingestion (committees, candidates, contributions)
ESSENTIAL_FILES = ["cm", "cn", "indiv"]

# Additional useful files
OPTIONAL_FILES = ["ccl", "pas2", "oth", "oppexp"]


def setup_logging() -> str:
    """Setup logging to both console and file in /tmp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"/tmp/download_bulk_data_{timestamp}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
    )

    return log_file


logger = logging.getLogger(__name__)


def download_file(url: str, output_path: Path, description: str = "") -> bool:
    """
    Download a file with progress tracking.

    Args:
        url: URL to download from
        output_path: Local path to save to
        description: Description for logging

    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Downloading {description or url}...")
        logger.info(f"  URL: {url}")
        logger.info(f"  Output: {output_path}")

        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()

        # Get file size if available
        total_size = int(response.headers.get("content-length", 0))
        size_mb = total_size / (1024 * 1024) if total_size else 0

        if total_size:
            logger.info(f"  Size: {size_mb:.1f} MB")

        # Download with progress tracking
        downloaded = 0
        chunk_size = 8192
        last_log_percent = 0

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)

                    # Log progress every 10%
                    if total_size:
                        percent = int((downloaded / total_size) * 100)
                        if percent >= last_log_percent + 10:
                            logger.info(f"  Progress: {percent}% ({downloaded / (1024*1024):.1f} MB)")
                            last_log_percent = percent

        logger.info(f"✓ Downloaded successfully: {output_path.name}")
        return True

    except requests.exceptions.RequestException as e:
        logger.error(f"✗ Download failed: {e}")
        return False


def extract_zip(zip_path: Path, output_dir: Path) -> bool:
    """
    Extract a ZIP file.

    Args:
        zip_path: Path to ZIP file
        output_dir: Directory to extract to

    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Extracting {zip_path.name}...")

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            # List contents
            members = zip_ref.namelist()
            logger.info(f"  Files in archive: {len(members)}")

            # Extract all
            zip_ref.extractall(output_dir)

        logger.info(f"✓ Extracted to: {output_dir}")
        return True

    except zipfile.BadZipFile as e:
        logger.error(f"✗ Invalid ZIP file: {e}")
        return False
    except Exception as e:
        logger.error(f"✗ Extraction failed: {e}")
        return False


def download_bulk_file(
    file_type: str,
    cycle: int,
    output_dir: Path,
    extract: bool = True,
    keep_zip: bool = False,
) -> bool:
    """
    Download and extract a bulk data file.

    Args:
        file_type: Type of file (e.g., 'cm', 'cn', 'indiv')
        cycle: Election cycle (e.g., 2026)
        output_dir: Directory to save files
        extract: Whether to extract ZIP files
        keep_zip: Whether to keep ZIP file after extraction

    Returns:
        True if successful, False otherwise
    """
    if file_type not in BULK_FILES:
        logger.error(f"Unknown file type: {file_type}")
        return False

    file_info = BULK_FILES[file_type]
    yy = cycle % 100  # Get last 2 digits (e.g., 2026 -> 26)

    # Format file names
    zip_name = file_info["zip_name"].format(yy=yy, cycle=cycle)
    zip_url = urljoin(FEC_BULK_DATA_URL, f"{cycle}/{zip_name}")
    header_url = file_info["header_url"]

    logger.info("=" * 80)
    logger.info(f"Processing: {file_info['description']}")
    logger.info("=" * 80)

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Download header file
    header_filename = f"{file_type}_header_file.csv"
    header_path = output_dir / header_filename

    if not download_file(header_url, header_path, f"{file_type} header file"):
        return False

    # Download ZIP file
    zip_path = output_dir / zip_name

    if not download_file(zip_url, zip_path, f"{file_type} data file"):
        return False

    # Extract if requested
    if extract:
        # Determine extraction directory
        if "extract_to_subdir" in file_info:
            extract_dir = output_dir / file_info["extract_to_subdir"].format(yy=yy)
            extract_dir.mkdir(parents=True, exist_ok=True)
        else:
            extract_dir = output_dir

        if not extract_zip(zip_path, extract_dir):
            return False

        # Remove ZIP file if not keeping
        if not keep_zip:
            logger.info(f"Removing ZIP file: {zip_path.name}")
            zip_path.unlink()

    logger.info(f"✓ Completed: {file_info['description']}\n")
    return True


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download FEC bulk data files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download essential files (committees, candidates, contributions)
  %(prog)s --cycle 2026 --output-dir data/2025-2026

  # Download all available files
  %(prog)s --cycle 2026 --output-dir data/2025-2026 --all

  # Download specific files
  %(prog)s --cycle 2026 --output-dir data/2025-2026 --files cm cn ccl

  # Download but don't extract (keep .zip files)
  %(prog)s --cycle 2026 --output-dir data/2025-2026 --no-extract

  # Extract but keep .zip files
  %(prog)s --cycle 2026 --output-dir data/2025-2026 --keep-zip

Available file types:
  Essential (required for basic ingestion):
    cm      - Committee Master File (~2 MB)
    cn      - Candidate Master File (<1 MB)
    indiv   - Individual Contributions (~700 MB - 2 GB) ⚠️  Large!

  Optional (additional data):
    ccl     - Candidate-Committee Linkages (<1 MB)
    pas2    - Committee-to-Candidate Contributions (~10 MB)
    oth     - Committee-to-Committee Transactions (~700 MB) ⚠️  Large!
    oppexp  - Operating Expenditures (~80 MB)
        """,
    )
    parser.add_argument(
        "--cycle",
        type=int,
        required=True,
        help="Election cycle year (e.g., 2026)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Directory to save downloaded files",
    )
    parser.add_argument(
        "--files",
        nargs="+",
        choices=list(BULK_FILES.keys()),
        help="Specific files to download (default: essential files only)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Download all available files (including optional)",
    )
    parser.add_argument(
        "--no-extract",
        action="store_true",
        help="Don't extract ZIP files (keep compressed)",
    )
    parser.add_argument(
        "--keep-zip",
        action="store_true",
        help="Keep ZIP files after extraction (default: delete)",
    )

    return parser.parse_args()


def main():
    # Setup logging
    log_file = setup_logging()
    logger.info(f"Logging to: {log_file}")
    logger.info(f"Stream logs with: tail -f {log_file}\n")

    args = parse_args()

    # Determine which files to download
    if args.all:
        files_to_download = list(BULK_FILES.keys())
        logger.info("Downloading ALL available files")
    elif args.files:
        files_to_download = args.files
        logger.info(f"Downloading specified files: {', '.join(files_to_download)}")
    else:
        files_to_download = ESSENTIAL_FILES
        logger.info(f"Downloading essential files: {', '.join(files_to_download)}")
        logger.info("(Use --all to download optional files)")

    output_dir = Path(args.output_dir)

    logger.info("")
    logger.info("=" * 80)
    logger.info("FEC BULK DATA DOWNLOAD")
    logger.info("=" * 80)
    logger.info(f"Cycle: {args.cycle}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Files to download: {len(files_to_download)}")
    logger.info(f"Extract: {not args.no_extract}")
    logger.info(f"Keep ZIP: {args.keep_zip}")
    logger.info("=" * 80)
    logger.info("")

    # Check for large files and warn
    large_files = [f for f in files_to_download if BULK_FILES[f].get("large", False)]
    if large_files:
        logger.warning("⚠️  WARNING: Downloading large files: " + ", ".join(large_files))
        logger.warning("   This may take significant time and disk space")
        logger.warning("")

    # Download each file
    success_count = 0
    failed_files = []

    for file_type in files_to_download:
        success = download_bulk_file(
            file_type=file_type,
            cycle=args.cycle,
            output_dir=output_dir,
            extract=not args.no_extract,
            keep_zip=args.keep_zip,
        )

        if success:
            success_count += 1
        else:
            failed_files.append(file_type)

    # Summary
    logger.info("=" * 80)
    logger.info("DOWNLOAD SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Successful: {success_count}/{len(files_to_download)}")
    logger.info(f"Failed: {len(failed_files)}/{len(files_to_download)}")

    if failed_files:
        logger.error(f"\n✗ Failed files: {', '.join(failed_files)}")

    logger.info("=" * 80)
    logger.info(f"\nLog file: {log_file}")

    if failed_files:
        logger.error("\n❌ Some downloads failed")
        sys.exit(1)
    else:
        logger.info("\n✅ All downloads completed successfully!")
        logger.info(f"\nNext step:")
        logger.info(f"  poetry run python scripts/run_bulk_ingestion.py \\")
        logger.info(f"    --data-dir {output_dir} \\")
        logger.info(f"    --cycle {args.cycle}")


if __name__ == "__main__":
    main()
