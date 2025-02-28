from ftplib import FTP, error_temp, error_perm, error_proto, error_reply
import os
import socket
import time
from threading import Thread, Lock
from tqdm import tqdm



def _is_dir(ftp: FTP, dir: str):
    try:
        ftp.cwd(dir)
        return True
    except:
        return False

def _get_file_size(ftp, filename):
    """Returns the size of a file on the FTP server."""
    try:
        return ftp.size(filename)
    except Exception as e:
        print(f"Could not retrieve size for {filename}: {e}")
        return None


def _download_file(ftp_host, ftp_dir, local_dir, filename, ftp_timeout, retry_limit, progress_bar, progress_lock):
    """Downloads a file, resuming if it's partially downloaded, and updates the tqdm progress bar."""
    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, filename)

    retries = 0
    while retries < retry_limit:
        try:
            with FTP(ftp_host, timeout=ftp_timeout) as ftp:
                ftp.login()
                ftp.cwd(ftp_dir)

                # Get remote file size
                remote_size = _get_file_size(ftp, filename)
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

                print(f"Downloaded: {os.path.join(ftp_dir, filename)}")
                with progress_lock:
                    progress_bar.update(1)
                return  # Success, exit loop

        except (socket.timeout, error_temp, error_perm, error_proto, error_reply) as e:
            retries += 1
            print(f"Retry {retries}/{retry_limit} for {filename}: {e}")
            time.sleep(2 ** retries)  # Exponential backoff
        except Exception as e:
            print(f"Unexpected error while downloading {filename}: {e}")
            break  # Stop trying on unknown errors

    print(f"Failed to download after {retry_limit} attempts: {filename}")
    with progress_lock:
        progress_bar.update(1)  # Mark as "processed" to avoid blocking progress


def download_parquet_files(ftp_host: str, ftp_dir: str, local_dir: str, ftp_timeout = 30, retry_limit = 3, n_threads = 4):
    """dDownload all .parquet files recursively using multiple threads with resume support and progress bar."""
    os.makedirs(local_dir, exist_ok=True)

    def get_files_recursively(ftp: FTP, dir: str):
        print(dir)
        ftp.cwd(dir)
        all_files = []
        for name in ftp.nlst():
            if _is_dir(ftp, os.path.join(dir, name)):
                all_files.extend(get_files_recursively(ftp, os.path.join(dir, name)))
            else:
                all_files.append((dir, name))
        return all_files

    try:
        with FTP(ftp_host, timeout=ftp_timeout) as ftp:
            ftp.login()
            all_files = get_files_recursively(ftp, ftp_dir)
    except (socket.timeout, error_temp, error_perm, error_proto, error_reply) as e:
        print(f"Failed to list files: {e}")
        return

    parquet_files = [f for f in all_files if f[1].endswith(".parquet")]
    print(f"Found {len(parquet_files)} .parquet files. Downloading...")

    # Lock for tqdm updates
    progress_lock = Lock()

    # Initialize tqdm progress bar
    with tqdm(total=len(parquet_files), desc="Downloading Files", unit="file") as progress_bar:
        # Multi-threaded download
        threads = []
        for i, (dir, filename) in enumerate(parquet_files):
            thread = Thread(target=_download_file, args=(ftp_host, dir, os.path.join(local_dir, dir.replace(ftp_dir, '', 1)), filename, ftp_timeout, retry_limit, progress_bar, progress_lock))
            threads.append(thread)
            thread.start()

            # Limit concurrent downloads
            if (i + 1) % n_threads == 0:
                for t in threads:
                    t.join()
                threads = []

        # Wait for remaining threads
        for t in threads:
            t.join()

    print("Download completed.")
