import subprocess
import time
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DB = os.getenv("MYSQL_DB", "credit_system")


def run_shell_command(command, wait=True):
    """Run a shell command and optionally wait for it to finish."""
    print(f"\nRunning: {command}")
    process = subprocess.Popen(command, shell=True)
    if wait:
        process.wait()


def run_in_background(command, label=""):
    """Run a command in background (no visible terminal)"""
    print(f"Starting {label} in background...")
    subprocess.Popen(command, shell=True)


if __name__ == "__main__":
    print("Rebuilding MySQL schema...")
    run_shell_command(
        f"mysql -u {MYSQL_USER} -p{MYSQL_PASSWORD} < credit_system_schema.sql"
    )

    print("Loading initial CSV data...")
    run_shell_command("python3 src/load_data.py")

    print("Launching Kafka consumer and producer...")
    run_in_background("python3 src/consumer.py", label="Kafka Consumer")
    time.sleep(2)  # Allow consumer to start first
    run_in_background("python3 src/producer.py", label="Kafka Producer")

    input(
        "\nPress Enter when Kafka processing is finished to continue with batch layer..."
    )

    print("Running batch processing...")
    run_shell_command("python3 src/batch_processing.py")

    print("Pushing updates to MySQL...")
    run_shell_command("python3 src/push_updates.py")

    print("\nAll steps completed.")
