# import os
# import subprocess
# import sys
# import pandas as pd

# # rename the database file to one with a suffix of the current date/time in ISO format (with ":" replaced by "-")
# timestamp = pd.Timestamp.now().isoformat().replace(":", "-")

# if os.path.exists("bio_data.duck.db"):
#     os.rename("bio_data.duck.db", f"bio_data.{timestamp}.duck.db")

# # Define the prefix start and end
# PREFIX_SCRIPT_START = "0000"
# PREFIX_SCRIPT_END   = "0800"  # e.g., "0130"

# time_start = pd.Timestamp.now()

# # Create logs folder if it doesn't exist
# os.makedirs("logs", exist_ok=True)

# # Build the log file path
# log_file_path = os.path.join("logs", f"runner_log.{timestamp}.log")

# # Path to the virtual environment's Python
# VENV_PYTHON = sys.executable  # Ensures the current Python interpreter is used

# # Directory where scripts are located
# script_dir = "."

# # Get list of Python files with numeric prefixes
# scripts = [f for f in os.listdir(script_dir) if f.endswith(".py") and f[:4].isdigit()]

# # Sort by the 4-digit prefix (as strings)
# scripts_sorted = sorted(scripts, key=lambda x: x[:4])

# # Filter based on PREFIX_SCRIPT_START and PREFIX_SCRIPT_END
# scripts_to_run = [f for f in scripts_sorted if PREFIX_SCRIPT_START <= f[:4] <= PREFIX_SCRIPT_END]

# # Function to log a line both to console and the log file
# def log_message(message: str):
#     print(message)
#     with open(log_file_path, "a", encoding="utf-8") as lf:
#         lf.write(message + "\n")

# for script in scripts_to_run:
#     start_subscript_time = pd.Timestamp.now()
#     log_message(f"=== Starting {script} at {start_subscript_time} ===")

#     # Run script, capturing stdout and stderr
#     process = subprocess.Popen(
#         [VENV_PYTHON, script],
#         stdout=subprocess.PIPE,
#         stderr=subprocess.PIPE,
#         text=True,             # or universal_newlines=True
#         encoding="utf-8",
#         errors="replace",      # This avoids UnicodeDecodeError
#     )
#     stdout, stderr = process.communicate()

#     returncode = process.returncode

#     # Write captured stdout/stderr to console & log
#     if stdout:
#         log_message(f"[stdout for {script}]:\n{stdout}")
#     if stderr:
#         log_message(f"[stderr for {script}]:\n{stderr}")

#     end_subscript_time = pd.Timestamp.now()
#     log_message(f"=== Ended {script} at {end_subscript_time} ===")
#     elapsed_time = end_subscript_time - start_subscript_time
#     log_message(f"Time taken for {script}: {elapsed_time}")

#     if returncode != 0:
#         log_message(f"❌ Error in {script} (exit code {returncode}) — stopping runner.")
#         break

# time_end = pd.Timestamp.now()
# total_runtime = time_end - time_start
# log_message(f"Time taken for all scripts: {total_runtime}")


# import os
# import subprocess
# import sys
# import pandas as pd
# import threading

# # rename the database file to one with a suffix of the current date/time in ISO format (with ":" replaced by "-")
# timestamp = pd.Timestamp.now().isoformat().replace(":", "-")

# if os.path.exists("bio_data.duck.db"):
#     os.rename("bio_data.duck.db", f"bio_data.{timestamp}.duck.db")

# # Define the prefix start and end
# PREFIX_SCRIPT_START = "0000"
# PREFIX_SCRIPT_END   = "0800"  # e.g., "0130"

# time_start = pd.Timestamp.now()

# # Create logs folder if it doesn't exist
# os.makedirs("logs", exist_ok=True)

# # Build the log file path
# log_file_path = os.path.join("logs", f"runner_log.{timestamp}.log")

# # Path to the virtual environment's Python
# VENV_PYTHON = sys.executable  # Ensures the current Python interpreter is used

# # Directory where scripts are located
# script_dir = "."

# # Get list of Python files with numeric prefixes
# scripts = [f for f in os.listdir(script_dir) if f.endswith(".py") and f[:4].isdigit()]

# # Sort by the 4-digit prefix (as strings)
# scripts_sorted = sorted(scripts, key=lambda x: x[:4])

# # Filter based on PREFIX_SCRIPT_START and PREFIX_SCRIPT_END
# scripts_to_run = [f for f in scripts_sorted if PREFIX_SCRIPT_START <= f[:4] <= PREFIX_SCRIPT_END]

# def log_message(message: str):
#     """Write a line to both console and the log file."""
#     print(message)
#     with open(log_file_path, "a", encoding="utf-8") as lf:
#         lf.write(message + "\n")

# def stream_output(pipe, log_func, prefix=""):
#     """
#     Reads lines from a given pipe (stdout or stderr) and logs them in real time.
#     Terminates when the pipe is closed.
#     """
#     try:
#         for line in iter(pipe.readline, ""):
#             if not line:
#                 break
#             line_str = line.rstrip("\n")
#             log_func(f"{prefix}{line_str}")
#     finally:
#         pipe.close()

# def run_script_in_real_time(command, log_func):
#     """
#     Launches a script in a subprocess and streams output line by line
#     to log_func (both stdout and stderr).
#     Returns the process' return code.
#     """
#     process = subprocess.Popen(
#         command,
#         stdout=subprocess.PIPE,
#         stderr=subprocess.PIPE,
#         text=True,             # Return strings (not bytes)
#         encoding="utf-8",
#         errors="replace"       # Avoid UnicodeDecodeError on invalid bytes
#     )

#     # Create threads to read from stdout and stderr in real time
#     t_stdout = threading.Thread(target=stream_output, args=(process.stdout, log_func, "[stdout] "))
#     t_stderr = threading.Thread(target=stream_output, args=(process.stderr, log_func, "[stderr] "))

#     # Start streaming threads
#     t_stdout.start()
#     t_stderr.start()

#     # Wait for process to complete
#     returncode = process.wait()

#     # Ensure our streaming threads finish
#     t_stdout.join()
#     t_stderr.join()

#     return returncode

# # Main execution loop
# for script in scripts_to_run:
#     start_subscript_time = pd.Timestamp.now()
#     log_message(f"=== Starting {script} at {start_subscript_time} ===")

#     # Run the script in real time
#     cmd = [VENV_PYTHON, script]
#     returncode = run_script_in_real_time(cmd, log_message)

#     end_subscript_time = pd.Timestamp.now()
#     log_message(f"=== Ended {script} at {end_subscript_time} ===")
#     elapsed_time = end_subscript_time - start_subscript_time
#     log_message(f"Time taken for {script}: {elapsed_time}")

#     if returncode != 0:
#         log_message(f"❌ Error in {script} (exit code {returncode}) — stopping runner.")
#         break

# time_end = pd.Timestamp.now()
# total_runtime = time_end - time_start
# log_message(f"Time taken for all scripts: {total_runtime}")



import os
import subprocess
import sys
import pandas as pd
import threading


# Ensure UTF-8 encoding
os.environ["PYTHONIOENCODING"] = "utf-8"


timestamp = pd.Timestamp.now().isoformat().replace(":", "-")

if os.path.exists("bio_data.duck.db"):
    os.rename("bio_data.duck.db", f"bio_data.{timestamp}.duck.db")

PREFIX_SCRIPT_START = "0000"
PREFIX_SCRIPT_END   = "0800"

time_start = pd.Timestamp.now()

os.makedirs("logs", exist_ok=True)
log_file_path = os.path.join("logs", f"runner_log.{timestamp}.log")

# Use the same Python interpreter, but add "-u" for unbuffered output
VENV_PYTHON = sys.executable  
PYTHON_CMD = [VENV_PYTHON, "-u"]  # -u for unbuffered output

script_dir = "."

scripts = [
    f for f in os.listdir(script_dir)
    if f.endswith(".py") and f[:4].isdigit()
]

scripts_sorted = sorted(scripts, key=lambda x: x[:4])
scripts_to_run = [
    f for f in scripts_sorted
    if PREFIX_SCRIPT_START <= f[:4] <= PREFIX_SCRIPT_END
]

def log_message(message: str):
    """Write a line to both console and the log file."""
    print(message)
    with open(log_file_path, "a", encoding="utf-8") as lf:
        lf.write(message + "\n")

def stream_output(pipe, prefix=""):
    """
    Reads lines from a given pipe (stdout or stderr) and logs them as they arrive.
    The prefix can distinguish [stdout] vs [stderr].
    """
    try:
        for line in iter(pipe.readline, ""):
            if not line:
                break
            # strip the trailing newline from the child process
            line_str = line.rstrip("\n")
            log_message(f"{prefix}{line_str}")
    finally:
        pipe.close()

def run_script_in_real_time(script_path):
    """
    Launches a script in a subprocess and streams output line by line
    as soon as it's available. Returns the process' return code.
    """
    process = subprocess.Popen(
        PYTHON_CMD + [script_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,       # Return strings rather than bytes
        encoding="utf-8",
        errors="replace" # Replace invalid UTF-8 chars instead of throwing an error
    )

    # Stream both stdout and stderr in real time via separate threads
    t_stdout = threading.Thread(target=stream_output, args=(process.stdout, f"[stdout {script_path}] "))
    t_stderr = threading.Thread(target=stream_output, args=(process.stderr, f"[stderr {script_path}] "))

    t_stdout.start()
    t_stderr.start()

    returncode = process.wait()  # Wait until the process finishes

    t_stdout.join()
    t_stderr.join()

    return returncode

for script in scripts_to_run:
    start_subscript_time = pd.Timestamp.now()
    log_message(f"=== Starting {script} at {start_subscript_time} ===")

    returncode = run_script_in_real_time(script)

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
