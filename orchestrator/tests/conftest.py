"""Shared pytest configuration.

Puts ``src`` on the import path so tests import ``services.*`` and ``api.*``
the same way the application does.
"""

import os
import sys

SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)
