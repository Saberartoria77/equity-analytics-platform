import schedule
import time
import subprocess
import logging
import sys
from datetime import datetime

# Configure logging to track task execution status
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scheduler.log"),  # Save logs to a file
        logging.StreamHandler(sys.stdout)  # Also output to console
    ]
)


def run_ingestion():
    """
    Wrapper function to execute the ingestion script.
    """
    logging.info("Task started: Daily data ingestion.")
    try:
        # Using subprocess to run ingest.py as a separate process.
        # This prevents environment pollution and handles memory better.
        result = subprocess.run(
            ["python", "ingest.py"],
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            logging.info("Task completed successfully.")
            # Optional: log the standard output if needed
            # logging.debug(f"Output: {result.stdout}")
        else:
            logging.error(f"Task failed with exit code {result.returncode}.")
            logging.error(f"Error Details:\n{result.stderr}")

    except Exception as e:
        logging.error(f"Unexpected error occurred in scheduler: {str(e)}")


# --- Scheduling Configuration ---

# Set the execution time (e.g., 02:00 AM)
# This is typically when server load is low and daily data is fully available.
TARGET_TIME = "02:00"

schedule.every().day.at(TARGET_TIME).do(run_ingestion)

logging.info(f"Scheduler initialized. Target time: {TARGET_TIME} every day.")

if __name__ == "__main__":
    try:
        while True:
            schedule.run_pending()
            # Sleep for 60 seconds to minimize CPU usage.
            # Precision isn't critical for a once-a-day task.
            time.sleep(60)
    except KeyboardInterrupt:
        logging.info("Scheduler stopped by user.")