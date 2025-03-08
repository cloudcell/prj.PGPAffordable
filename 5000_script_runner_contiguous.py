import os
import subprocess
import sys
import pandas as pd

# rename the database file to one with a suffix of the current date/time in ISO format (with ":" replaced by "-")
timestamp = pd.Timestamp.now().isoformat().replace(":", "-")

if os.path.exists("bio_data.duck.db"):
    os.rename("bio_data.duck.db", f"bio_data.{timestamp}.duck.db")

# Define the prefix start and end
PREFIX_SCRIPT_START = "0000"
PREFIX_SCRIPT_END   = "0800"  # e.g., "0130"

time_start = pd.Timestamp.now()

# Create logs folder if it doesn't exist
os.makedirs("logs", exist_ok=True)

# Build the log file path
log_file_path = os.path.join("logs", f"runner_log.{timestamp}.log")

# Path to the virtual environment's Python
VENV_PYTHON = sys.executable  # Ensures the current Python interpreter is used

# Directory where scripts are located
script_dir = "."

# Get list of Python files with numeric prefixes
scripts = [f for f in os.listdir(script_dir) if f.endswith(".py") and f[:4].isdigit()]

# Sort by the 4-digit prefix (as strings)
scripts_sorted = sorted(scripts, key=lambda x: x[:4])

# Filter based on PREFIX_SCRIPT_START and PREFIX_SCRIPT_END
scripts_to_run = [f for f in scripts_sorted if PREFIX_SCRIPT_START <= f[:4] <= PREFIX_SCRIPT_END]

# Function to log a line both to console and the log file
def log_message(message: str):
    print(message)
    with open(log_file_path, "a", encoding="utf-8") as lf:
        lf.write(message + "\n")

for script in scripts_to_run:
    start_subscript_time = pd.Timestamp.now()
    log_message(f"=== Starting {script} at {start_subscript_time} ===")

    # Run script, capturing stdout and stderr
    process = subprocess.Popen(
        [VENV_PYTHON, script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,             # or universal_newlines=True
        encoding="utf-8",
        errors="replace",      # This avoids UnicodeDecodeError
    )
    stdout, stderr = process.communicate()

    returncode = process.returncode

    # Write captured stdout/stderr to console & log
    if stdout:
        log_message(f"[stdout for {script}]:\n{stdout}")
    if stderr:
        log_message(f"[stderr for {script}]:\n{stderr}")

    end_subscript_time = pd.Timestamp.now()
    log_message(f"=== Ended {script} at {end_subscript_time} ===")
    elapsed_time = end_subscript_time - start_subscript_time
    log_message(f"Time taken for {script}: {elapsed_time}")

    if returncode != 0:
        log_message(f"❌ Error in {script} (exit code {returncode}) — stopping runner.")
        break

time_end = pd.Timestamp.now()
total_runtime = time_end - time_start
log_message(f"Time taken for all scripts: {total_runtime}")
