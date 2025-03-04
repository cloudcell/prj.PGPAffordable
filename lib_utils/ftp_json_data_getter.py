from hashlib import md5
from ftplib import FTP, error_temp, error_perm, error_proto, error_reply
import os
import socket
import time
from threading import Thread, Lock
from tqdm import tqdm



def _get_file_size(ftp, filename):
    """Returns the size of a file on the FTP server."""
    try:
        return ftp.size(filename)
    except Exception as e:
        print(f"Could not retrieve size for {filename}: {e}")
        return None


def _get_hash_of_file(path_to_file):
    with open(path_to_file, 'rb') as f:
        file_hash = md5(f.read()).hexdigest()
    return file_hash


def _save_hash_of_file(path_to_file, path_to_hash):
    file_hash = _get_hash_of_file(path_to_file)
    with open(path_to_hash, 'w') as f:
        f.write(file_hash)


def _download_file(ftp_host, ftp_dir, local_dir, filename, ftp_timeout, retry_limit, progress_bar, progress_lock):
    """Downloads a file, resuming if it's partially downloaded, and updates the tqdm progress bar."""
    local_path = os.path.join(local_dir, filename)
    path_to_hash = local_path + '.md5'

    if os.path.exists(path_to_hash):
        file_hash = _get_hash_of_file(local_path)

        with open(path_to_hash) as f:
            saved_hash = f.read()
        
        if file_hash == saved_hash:
            print(f"Skipping (already complete): {filename}")
            with progress_lock:
                progress_bar.update(1)
            return

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
                        _save_hash_of_file(local_path, path_to_hash)
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

                _save_hash_of_file(local_path, path_to_hash)
                print(f"Downloaded: {filename}")
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


def download_json_files(ftp_host: str, ftp_dir: str, local_dir: str, ftp_timeout = 30, retry_limit = 8, n_threads = 4):
    """Lists all JSON files and downloads them using multiple threads with resume support and progress bar."""
    os.makedirs(local_dir, exist_ok=True)

    try:
        with FTP(ftp_host, timeout=ftp_timeout) as ftp:
            ftp.login()
            ftp.cwd(ftp_dir)
            files = ftp.nlst()
    except (socket.timeout, error_temp, error_perm, error_proto, error_reply) as e:
        print(f"Failed to list files: {e}")
        return

    json_files = [f for f in files if f.endswith(".json")]
    print(f"Found {len(json_files)} JSON files. Downloading...")

    # Lock for tqdm updates
    progress_lock = Lock()

    # Initialize tqdm progress bar
    with tqdm(total=len(json_files), desc="Downloading Files", unit="file") as progress_bar:
        # Multi-threaded download
        threads = []
        for i, filename in enumerate(json_files):
            thread = Thread(target=_download_file, args=(ftp_host, ftp_dir, local_dir, filename, ftp_timeout, retry_limit, progress_bar, progress_lock))
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
