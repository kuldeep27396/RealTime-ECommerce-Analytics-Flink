"""
Entry point for the producer service
"""

import os
import sys

# Add src directory to path
sys.path.insert(0, os.path.dirname(__file__))

from producer import main

if __name__ == "__main__":
    main()
