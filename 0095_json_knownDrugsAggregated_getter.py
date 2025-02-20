from ftplib import FTP, error_temp, error_perm, error_proto, error_reply
import os
import socket
import time
from threading import Thread, Lock
from tqdm import tqdm

# FTP server details
FTP_HOST = "ftp.ebi.ac.uk"
FTP_DIR = "/pub/databases/opentargets/platform/24.09/output/etl/json/knownDrugsAggregated/"
LOCAL_DIR = "data/202409XX/knownDrugsAggregated"  # Change this to your desired local directory

# Ensure local directory exists
os.makedirs(LOCAL_DIR, exist_ok=True)

# Configurable parameters
FTP_TIMEOUT = 30  # Timeout in seconds
RETRY_LIMIT = 3  # Max retries per file
THREADS = 4  # Number of concurrent downloads

# Lock for tqdm updates
progress_lock = Lock()


def get_file_size(ftp, filename):
    """Returns the size of a file on the FTP server."""
    try:
        return ftp.size(filename)
    except Exception as e:
        print(f"Could not retrieve size for {filename}: {e}")
        return None


def download_file(filename, progress_bar):
    """Downloads a file, resuming if it's partially downloaded, and updates the tqdm progress bar."""
    local_path = os.path.join(LOCAL_DIR, filename)

    retries = 0
    while retries < RETRY_LIMIT:
        try:
            with FTP(FTP_HOST, timeout=FTP_TIMEOUT) as ftp:
                ftp.login()
                ftp.cwd(FTP_DIR)

                # Get remote file size
                remote_size = get_file_size(ftp, filename)
                if remote_size is None:
                    print(f"Skipping {filename}: Could not determine file size.")
                    return

                # Check if the file already exists
                if os.path.exists(local_path):
                    local_size = os.path.getsize(local_path)

                    if local_size == remote_size:
                        print(f"Skipping (already complete): {filename}")
                        with progress_lock:
                            progress_bar.update(1)
                        return
                    elif local_size < remote_size:
                        print(f"Resuming {filename}: Local ({local_size} bytes) < Remote ({remote_size} bytes)")
                    else:
                        print(f"Warning: Local file {filename} is larger than the remote version. Overwriting.")
                        local_size = 0  # Start from scratch

                else:
                    local_size = 0  # Start fresh

                # Open file and resume download
                with open(local_path, "ab") as f:
                    if local_size > 0:
                        ftp.sendcmd(f"REST {local_size}")  # Resume from last downloaded byte

                    ftp.retrbinary(f"RETR {filename}", f.write, rest=local_size)

                print(f"Downloaded: {filename}")
                with progress_lock:
                    progress_bar.update(1)
                return  # Success, exit loop

        except (socket.timeout, error_temp, error_perm, error_proto, error_reply) as e:
            retries += 1
            print(f"Retry {retries}/{RETRY_LIMIT} for {filename}: {e}")
            time.sleep(2 ** retries)  # Exponential backoff
        except Exception as e:
            print(f"Unexpected error while downloading {filename}: {e}")
            break  # Stop trying on unknown errors

    print(f"Failed to download after {RETRY_LIMIT} attempts: {filename}")
    with progress_lock:
        progress_bar.update(1)  # Mark as "processed" to avoid blocking progress


def download_json_files():
    """Lists all JSON files and downloads them using multiple threads with resume support and progress bar."""
    try:
        with FTP(FTP_HOST, timeout=FTP_TIMEOUT) as ftp:
            ftp.login()
            ftp.cwd(FTP_DIR)
            files = ftp.nlst()
    except (socket.timeout, error_temp, error_perm, error_proto, error_reply) as e:
        print(f"Failed to list files: {e}")
        return

    json_files = [f for f in files if f.endswith(".json")]
    print(f"Found {len(json_files)} JSON files. Downloading...")

    # Initialize tqdm progress bar
    with tqdm(total=len(json_files), desc="Downloading Files", unit="file") as progress_bar:
        # Multi-threaded download
        threads = []
        for i, filename in enumerate(json_files):
            thread = Thread(target=download_file, args=(filename, progress_bar))
            threads.append(thread)
            thread.start()

            # Limit concurrent downloads
            if (i + 1) % THREADS == 0:
                for t in threads:
                    t.join()
                threads = []

        # Wait for remaining threads
        for t in threads:
            t.join()

    print("Download completed.")


if __name__ == "__main__":
    download_json_files()
