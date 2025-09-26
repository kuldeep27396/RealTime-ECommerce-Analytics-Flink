"""
Entry point for the producer service
"""

import os
import sys

# Add src directory to path
sys.path.insert(0, os.path.dirname(__file__))

from simple_producer import SimpleProducer

def main():
    producer = SimpleProducer()
    producer.run()

if __name__ == "__main__":
    main()
