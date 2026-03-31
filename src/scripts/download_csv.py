#!/usr/bin/env python3
"""
Download credit card fraud dataset from Kaggle.

This script downloads the creditcardfraud dataset and extracts it to the dados/ directory.
Works on Windows, macOS, and Linux.
"""

import os
import subprocess
import sys
from pathlib import Path


def main():
    """Download and extract the credit card fraud dataset."""
    # Get the directory where this script is located
    script_dir = Path(__file__).resolve().parent
    data_dir = script_dir.parent.parent / "dados"
    
    # Create dados directory if it doesn't exist
    data_dir.mkdir(parents=True, exist_ok=True)
    print(f"Data directory: {data_dir}")
    
    # Download dataset using kaggle CLI
    try:
        cmd = [
            "uvx",
            "kaggle",
            "datasets",
            "download",
            "-d",
            "mlg-ulb/creditcardfraud",
            "-p",
            str(data_dir),
            "--unzip",
        ]
        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, check=True)
        print("✓ Dataset downloaded and extracted successfully!")
        return result.returncode
    except subprocess.CalledProcessError as e:
        print(f"✗ Error downloading dataset: {e}", file=sys.stderr)
        return 1
    except FileNotFoundError:
        print(
            "✗ Error: 'uvx' command not found. Please install uv first: pip install uv",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
