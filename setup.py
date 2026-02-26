#!/usr/bin/env python3
"""
Quick setup and management script for Migration Platform Phase 1
"""

import subprocess
import sys
import time
import requests
from typing import Optional

def run_command(cmd: str, description: str) -> bool:
    """Run a shell command and print status"""
    print(f"\n{'='*60}")
    print(f"▶ {description}")
    print(f"{'='*60}")
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=False)
        print(f"✓ {description} - SUCCESS")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ {description} - FAILED")
        return False

def check_health(max_retries: int = 30) -> bool:
    """Check if API is healthy"""
    url = "http://localhost:8000/health"
    for i in range(max_retries):
        try:
            response = requests.get(url, timeout=2)
            if response.status_code == 200:
                print(f"✓ API is healthy: {response.json()}")
                return True
        except:
            pass
        print(f"Waiting for API to be ready... ({i+1}/{max_retries})")
        time.sleep(2)
    return False

def main():
    if len(sys.argv) < 2:
        print("""
Migration Platform - Phase 1 Management Script

Usage:
    python setup.py [command] [options]

Commands:
    start [N]       - Start platform with N workers (default: 4)
    stop            - Stop all services
    restart [N]     - Restart with N workers
    logs [service]  - View logs (api, worker, postgres, redis, all)
    status          - Show service status
    health          - Check API health
    scale N         - Scale workers to N instances
    clean           - Stop and remove all data (DESTRUCTIVE!)
    init-db         - Initialize database schema
    
Examples:
    python setup.py start 4
    python setup.py logs worker
    python setup.py scale 8
    python setup.py health
        """)
        sys.exit(0)

    command = sys.argv[1].lower()

    if command == "start":
        workers = int(sys.argv[2]) if len(sys.argv) > 2 else 4
        run_command("docker-compose up -d", "Starting services")
        run_command(f"docker-compose up -d --scale worker={workers}", f"Scaling to {workers} workers")
        print("\nWaiting for services to be ready...")
        if check_health():
            print("\n✓ Platform is ready!")
            print(f"  API: http://localhost:8000")
            print(f"  Docs: http://localhost:8000/docs")
            print(f"  Workers: {workers}")
        else:
            print("\n✗ API health check failed. Check logs with: python setup.py logs")

    elif command == "stop":
        run_command("docker-compose down", "Stopping services")

    elif command == "restart":
        workers = int(sys.argv[2]) if len(sys.argv) > 2 else 4
        run_command("docker-compose down", "Stopping services")
        run_command("docker-compose up -d", "Starting services")
        run_command(f"docker-compose up -d --scale worker={workers}", f"Scaling to {workers} workers")
        check_health()

    elif command == "logs":
        service = sys.argv[2] if len(sys.argv) > 2 else ""
        if service:
            subprocess.run(f"docker-compose logs -f {service}", shell=True)
        else:
            subprocess.run("docker-compose logs -f", shell=True)

    elif command == "status":
        subprocess.run("docker-compose ps", shell=True)

    elif command == "health":
        if check_health(max_retries=5):
            # Also get metrics
            try:
                response = requests.get("http://localhost:8000/metrics", timeout=2)
                print(f"\nMetrics: {response.json()}")
            except:
                pass
        else:
            print("✗ API is not healthy")
            sys.exit(1)

    elif command == "scale":
        if len(sys.argv) < 3:
            print("Error: Specify number of workers. Example: python setup.py scale 8")
            sys.exit(1)
        workers = int(sys.argv[2])
        run_command(f"docker-compose up -d --scale worker={workers}", f"Scaling to {workers} workers")

    elif command == "clean":
        print("⚠️  WARNING: This will delete all migration data!")
        confirm = input("Are you sure? Type 'yes' to confirm: ")
        if confirm.lower() == "yes":
            run_command("docker-compose down -v", "Removing all containers and volumes")
            print("✓ Cleanup complete")
        else:
            print("Cancelled")

    elif command == "init-db":
        print("Initializing database schema...")
        run_command(
            "docker exec -i migration_metadata_db psql -U postgres -d migration_metadata < schema.sql",
            "Loading schema"
        )

    else:
        print(f"Unknown command: {command}")
        print("Run 'python setup.py' for usage help")
        sys.exit(1)

if __name__ == "__main__":
    main()
