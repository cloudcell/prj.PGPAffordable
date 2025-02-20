from ftplib import FTP
import os
from tqdm import tqdm

# FTP server details
FTP_HOST = "ftp.ebi.ac.uk"
FTP_DIR = "/pub/databases/opentargets/platform/24.09/output/etl/json/molecule/"
LOCAL_DIR = "data/202409XX/molecule"  # Change this to your desired local directory

# Ensure local directory exists
os.makedirs(LOCAL_DIR, exist_ok=True)

def get_remote_file_size(ftp, filename):
    """Get the size of a remote file on the FTP server."""
    try:
        return ftp.size(filename)
    except Exception:
        return None

def download_json_files():
    """Connects to the FTP server, lists all JSON files, and downloads them with a progress bar.
       It checks if the file is already downloaded and resumes if incomplete."""
    ftp = FTP(FTP_HOST)
    ftp.login()
    ftp.cwd(FTP_DIR)
    
    # List files in the directory
    files = ftp.nlst()
    json_files = [f for f in files if f.endswith(".json")]
    
    print(f"Found {len(json_files)} JSON files. Downloading...")
    
    with tqdm(total=len(json_files), desc="Downloading JSON files", unit="file") as pbar:
        for filename in json_files:
            local_path = os.path.join(LOCAL_DIR, filename)
            remote_size = get_remote_file_size(ftp, filename)
            local_size = os.path.getsize(local_path) if os.path.exists(local_path) else 0
            
            if local_size == remote_size:
                pbar.update(1)
                continue  # Skip download if file already exists and is complete
            
            mode = "ab" if local_size > 0 else "wb"  # Append mode if resuming download
            with open(local_path, mode) as f:
                ftp.retrbinary(f"RETR {filename}", f.write, rest=local_size)
            
            pbar.update(1)
    
    ftp.quit()
    print("Download completed.")

if __name__ == "__main__":
    download_json_files()
