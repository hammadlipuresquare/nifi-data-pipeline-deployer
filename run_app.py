#!/usr/bin/env python3
"""
Universal entry point for running different parts of the application.
Handles module imports correctly for PyCharm debugging.
"""

import sys
import os

# Add project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

def run_orchestrator_app():
    """Run the orchestrator app."""
    from src.orchestrator.app import main
    main()

def run_orchestrator_demo():
    """Run the orchestrator in demo mode."""
    sys.argv = ['app.py', '--demo']  # Simulate --demo argument
    from src.orchestrator.app import main
    main()

def run_main():
    """Run the original main.py (Kafka consumer + manual fallback)."""
    import main  # This will execute main.py

def run_kafka_consumer():
    """Run just the Kafka consumer."""
    from simple_kafka_main import start_kafka_listener
    start_kafka_listener()

def run_debug_integration():
    """Run integration debugging."""
    from debug_integration import debug_integration_deployment
    debug_integration_deployment()

def run_debug_kafka():
    """Run Kafka debugging."""
    from debug_kafka import debug_kafka_processing
    debug_kafka_processing()

if __name__ == "__main__":
    print("🚀 Universal App Runner")
    print("="*50)
    
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
        
        if mode == "orchestrator" or mode == "app":
            print("Running Orchestrator App...")
            run_orchestrator_app()
        elif mode == "demo":
            print("Running Orchestrator Demo...")
            run_orchestrator_demo()
        elif mode == "main":
            print("Running Main (Kafka + Manual)...")
            run_main()
        elif mode == "kafka":
            print("Running Kafka Consumer...")
            run_kafka_consumer()
        elif mode == "debug-integration":
            print("Running Integration Debug...")
            run_debug_integration()
        elif mode == "debug-kafka":
            print("Running Kafka Debug...")
            run_debug_kafka()
        else:
            print(f"Unknown mode: {mode}")
            print("Available modes: orchestrator, demo, main, kafka, debug-integration, debug-kafka")
    else:
        # Default: run orchestrator app
        print("Running Orchestrator App (default)...")
        run_orchestrator_app()


