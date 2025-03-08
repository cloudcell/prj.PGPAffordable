from lib_utils.ftp_parquet_data_getter import download_parquet_files

# FTP server details
FTP_HOST = "ftp.ebi.ac.uk"
FTP_DIR = "/pub/databases/opentargets/platform/24.09/output/etl/parquet/evidence/"
LOCAL_DIR = "data/202409XX/evidence"  # Change this to your desired local directory
CHECKSUM_FILE = 'data/202409XX/release_data_integrity'

download_parquet_files(FTP_HOST, FTP_DIR, LOCAL_DIR, CHECKSUM_FILE)
