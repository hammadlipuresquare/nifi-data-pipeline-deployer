#!/usr/bin/env python3
"""
Entry point script for the orchestrator application.
This can be run directly and handles the import path correctly.
"""

import sys
import os

# Add the project root to Python path so imports work
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Now we can import the app using absolute imports
from src.orchestrator.app import main

if __name__ == "__main__":
    main()
