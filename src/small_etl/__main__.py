"""Entry point for running small_etl as a module.

Usage:
    python -m small_etl run [options]
    python -m small_etl assets [options]
    python -m small_etl trades [options]
"""

import sys

from small_etl.cli import main

if __name__ == "__main__":
    sys.exit(main())
