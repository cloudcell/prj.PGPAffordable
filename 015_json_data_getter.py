from ftplib import FTP
import os

# FTP server details
FTP_HOST = "ftp.ebi.ac.uk"
FTP_DIR = "/pub/databases/opentargets/platform/24.09/output/etl/json/molecule/"
LOCAL_DIR = "data/202409XX/molecule"  # Change this to your desired local directory

# Ensure local directory exists
os.makedirs(LOCAL_DIR, exist_ok=True)

def download_json_files():
    """Connects to the FTP server, lists all JSON files, and downloads them."""
    ftp = FTP(FTP_HOST)
    ftp.login()
    ftp.cwd(FTP_DIR)
    
    # List files in the directory
    files = ftp.nlst()
    json_files = [f for f in files if f.endswith(".json")]
    
    print(f"Found {len(json_files)} JSON files. Downloading...")
    
    for filename in json_files:
        local_path = os.path.join(LOCAL_DIR, filename)
        
        with open(local_path, "wb") as f:
            ftp.retrbinary(f"RETR {filename}", f.write)
        print(f"Downloaded: {filename}")
    
    ftp.quit()
    print("Download completed.")

if __name__ == "__main__":
    download_json_files()
