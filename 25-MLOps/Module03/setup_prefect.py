#!/usr/bin/env python3
"""
Setup script for Prefect configuration and deployment.
Run this to initialize your Prefect environment for the horse racing pipeline.
"""

import subprocess
import sys
from pathlib import Path


def run_command(command: str, description: str):
    """Run a shell command and handle errors."""
    print(f"🔧 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        if result.stdout:
            print(f"Output: {result.stdout.strip()}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e}")
        if e.stderr:
            print(f"Error: {e.stderr.strip()}")
        return False


def setup_directories():
    """Create necessary directories."""
    directories = ["raw_data", "processed_data", "logs"]
    for dir_name in directories:
        Path(dir_name).mkdir(exist_ok=True)
        print(f"📁 Created directory: {dir_name}")


def main():
    """Main setup function."""
    print("🏇 Setting up Horse Racing MLOps Pipeline with Prefect")
    print("=" * 55)
    
    # Create directories
    setup_directories()
    
    # Start Prefect server (this will run in the background)
    print("\n🚀 Starting Prefect server...")
    print("This will start the Prefect UI at http://localhost:4200")
    print("You can access the dashboard there to monitor your flows.")
    
    # Check if Prefect is installed
    if not run_command("prefect version", "Checking Prefect installation"):
        print("Please install Prefect: uv add prefect")
        return
    
    # Start Prefect server
    print("\n🔧 To start the Prefect server, run:")
    print("prefect server start")
    print("\n🔧 In a separate terminal, run your pipeline:")
    print("python grab_data.py")
    
    print("\n🔧 Or schedule it to run daily:")
    print("prefect deployment build grab_data.py:grab_horse_racing_data -n 'Daily Horse Racing Data'")
    print("prefect deployment apply grab_horse_racing_data-deployment.yaml")
    
    print("\n🎉 Setup complete! Check the README for next steps.")


if __name__ == "__main__":
    main()