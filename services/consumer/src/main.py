"""
Entry point for the consumer service
"""

import os
import sys

# Add src directory to path
sys.path.insert(0, os.path.dirname(__file__))

from simple_consumer import SimpleFlinkConsumer

def main():
    consumer = SimpleFlinkConsumer()
    consumer.run()

if __name__ == "__main__":
    main()
