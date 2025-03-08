# get the file from ftp server and calculate the checksum
# /ftp.ebi.ac.uk/pub/databases/opentargets/platform/24.09/release_data_integrity
# /ftp.ebi.ac.uk/pub/databases/opentargets/platform/24.09/release_data_integrity.sha1

import ftplib
import hashlib
import os

# Define FTP details
FTP_SERVER = "ftp.ebi.ac.uk"
REMOTE_DIR = "/pub/databases/opentargets/platform/24.09/"
FILE_NAME = "release_data_integrity"
CHECKSUM_FILE_NAME = "release_data_integrity.sha1"
LOCAL_FOLDER = "./data/202409XX/"


# Local file paths
LOCAL_FILE = os.path.join(os.getcwd(), LOCAL_FOLDER, FILE_NAME)
LOCAL_CHECKSUM_FILE = os.path.join(os.getcwd(), LOCAL_FOLDER, CHECKSUM_FILE_NAME)

def download_file(ftp, remote_path, local_path):
    """Download a file from the FTP server."""
    with open(local_path, "wb") as f:
        ftp.retrbinary(f"RETR {remote_path}", f.write)

def calculate_sha1(file_path):
    """Calculate SHA-1 checksum of a local file."""
    sha1 = hashlib.sha1()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha1.update(chunk)
    return sha1.hexdigest()

# check if the file exists and do not download it again unless the checksum is different
if os.path.exists(LOCAL_FILE):
    # Calculate actual SHA-1 checksum
    actual_checksum = calculate_sha1(LOCAL_FILE)
    # Read expected checksum from file
    with open(LOCAL_CHECKSUM_FILE, "r") as f:
        expected_checksum = f.read().strip().split()[0]  # Extract checksum value
    # Verify checksum
    if actual_checksum == expected_checksum:
        print("✅ Checksum verification successful: File is intact.")
        exit(0)
    else:
        print("❌ Checksum mismatch: File may be corrupted.")
        os.remove(LOCAL_FILE)
        os.remove(LOCAL_CHECKSUM_FILE)
        

# Connect to FTP server
ftp = ftplib.FTP(FTP_SERVER)
ftp.login()

# Change to the correct directory
ftp.cwd(REMOTE_DIR)

# Download the data file
print(f"🔄 Downloading {FILE_NAME}...")
download_file(ftp, FILE_NAME, LOCAL_FILE)
print(f"✅ {FILE_NAME} downloaded successfully.")

# Download the checksum file
print(f"🔄 Downloading {CHECKSUM_FILE_NAME}...")
download_file(ftp, CHECKSUM_FILE_NAME, LOCAL_CHECKSUM_FILE)
print(f"✅ {CHECKSUM_FILE_NAME} downloaded successfully.")

# Read expected checksum from file
with open(LOCAL_CHECKSUM_FILE, "r") as f:
    expected_checksum = f.read().strip().split()[0]  # Extract checksum value

# Calculate actual SHA-1 checksum
actual_checksum = calculate_sha1(LOCAL_FILE)

# Verify checksum
if actual_checksum == expected_checksum:
    print("✅ Checksum verification successful: File is intact.")
else:
    print("❌ Checksum mismatch: File may be corrupted.")
    exit(1)

# Close FTP connection
ftp.quit()
