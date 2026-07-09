#!/usr/bin/env python3
"""Worker entry point."""

import sys
import os

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from worker_cli import main

if __name__ == "__main__":
    sys.exit(main())
