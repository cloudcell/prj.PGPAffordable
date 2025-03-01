import os
import subprocess
import sys

# Define the prefix start and end
PREFIX_SCRIPT_START = "0000"
PREFIX_SCRIPT_END = "0800"  # "0130"

# Path to the virtual environment's Python
VENV_PYTHON = sys.executable  # This ensures the current Python interpreter is used

# Get list of Python files with numeric prefixes
script_dir = "."  # Change this if scripts are in a different directory
scripts = [f for f in os.listdir(script_dir) if f.endswith(".py") and f[:4].isdigit()]

# Extract prefixes and sort as strings
scripts_sorted = sorted(scripts, key=lambda x: x[:4])

# Filter based on PREFIX_SCRIPT_START and PREFIX_SCRIPT_END
scripts_to_run = [f for f in scripts_sorted if PREFIX_SCRIPT_START <= f[:4] <= PREFIX_SCRIPT_END]

# Execute scripts sequentially and stop on failure
for script in scripts_to_run:
    print(f"Running: {script}")
    process = subprocess.Popen([VENV_PYTHON, script], stdout=sys.stdout, stderr=sys.stderr)

    returncode = process.wait()  # Wait for process to complete

    if returncode != 0:
        print(f"❌ Error in {script} (exit code {returncode})")
        break
