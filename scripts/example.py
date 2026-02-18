#!/usr/bin/env python3
"""
Example script to demonstrate the Docker setup.
This script shows basic functionality that can be run via the Docker container.
"""

import sys
from datetime import datetime


def main():
    print("Hello from the Docker container!")
    print(f"Current time: {datetime.now()}")
    print(f"Python version: {sys.version}")
    print(f"Script arguments: {sys.argv[1:] if len(sys.argv) > 1 else 'None'}")

    # Example of using the caveclient dependency
    try:
        import caveclient

        print(f"CaveClient version: {caveclient.__version__}")
    except ImportError:
        print("CaveClient not available")

    print("Script completed successfully!")


if __name__ == "__main__":
    main()
