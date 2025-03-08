from hashlib import md5, sha1
from ftplib import FTP, error_temp, error_perm, error_proto, error_reply
import os
import socket
import time
from threading import Thread, Lock
from tqdm import tqdm


_has_errors = False # flag to interrupt process


def _get_hash_of_file(path_to_file):
    with open(path_to_file, 'rb') as f:
        # file_hash = md5(f.read()).hexdigest()
        file_hash = sha1(f.read()).hexdigest()
    return file_hash


def _download_file(ftp_host, ftp_dir, local_dir, filename, saved_hash, remote_size, ftp_timeout, retry_limit, progress_bar, progress_lock):
    """Downloads a file, resuming if it's partially downloaded, and updates the tqdm progress bar."""
    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, filename)
    if os.path.exists(local_path):
        file_hash = _get_hash_of_file(local_path)
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

                # Check if the file already exists
                if os.path.exists(local_path):
                    local_size = os.path.getsize(local_path)
                    if local_size < remote_size:
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
    global _has_errors
    _has_errors = True


def download_parquet_files(ftp_host: str, ftp_dir: str, local_dir: str, checksum_file: str, ftp_timeout = 30, retry_limit = 10, n_threads = 4):
    """dDownload all .parquet files recursively using multiple threads with resume support and progress bar."""
    os.makedirs(local_dir, exist_ok=True)

    if not os.path.exists(checksum_file):
        print(f"error: {checksum_file} not exists")
        exit(1)

    with open(checksum_file) as f:
        checksum_dict = dict(reversed(line.split(maxsplit=1)) for line in f.read().split('\n') if './output/etl/' in line)

    # filter by ftp_dir to optimize dict size
    rel_path = ftp_dir.split('/output/etl/', maxsplit=1)[-1]
    checksum_dict = {k.replace('./output/etl/', ''): v  for k, v in checksum_dict.items() if rel_path in k}

    def get_files_recursively(ftp: FTP, dir: str):
        print(dir)
        ftp.cwd(dir)
        ftp_list = []
        ftp.retrlines('LIST', ftp_list.append)

        all_files = []
        for line in ftp_list:
            parts = line.split(maxsplit=8)  # Ensures filenames with spaces are preserved
            name = parts[-1]
            size = int(parts[4])
            if line.startswith('d'):
                all_files.extend(get_files_recursively(ftp, os.path.join(dir, name)))
            else:
                all_files.append((dir, name, size))
        return all_files

    try:
        with FTP(ftp_host, timeout=ftp_timeout) as ftp:
            ftp.login()
            all_files = get_files_recursively(ftp, ftp_dir)
    except (socket.timeout, error_temp, error_perm, error_proto, error_reply) as e:
        print(f"Failed to list files: {e}")
        exit(1)

    parquet_files = [f for f in all_files if f[1].endswith(".parquet")]
    print(f"Found {len(parquet_files)} .parquet files. Downloading...")

    # Lock for tqdm updates
    progress_lock = Lock()

    # Initialize tqdm progress bar
    with tqdm(total=len(parquet_files), desc="Downloading Files", unit="file") as progress_bar:
        # Multi-threaded download
        threads = []
        for i, (dir, filename, size) in enumerate(parquet_files):
            saved_hash = checksum_dict[os.path.join(dir, filename).split('/output/etl/', maxsplit=1)[-1].replace('\\', '/')]

            thread = Thread(daemon=True, target=_download_file, args=(ftp_host, dir, os.path.join(local_dir, dir.replace(ftp_dir, '', 1)), filename, saved_hash, size, ftp_timeout, retry_limit, progress_bar, progress_lock))
            threads.append(thread)
            thread.start()

            # Limit concurrent downloads
            if (i + 1) % n_threads == 0:
                for t in threads:
                    t.join()
                    if _has_errors:
                        exit(1)
                threads = []

        # Wait for remaining threads
        for t in threads:
            t.join()
            if _has_errors:
                exit(1)

    print("Download completed.")
