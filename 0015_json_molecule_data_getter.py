from lib_utils.ftp_json_data_getter import download_json_files

# FTP server details
FTP_HOST = "ftp.ebi.ac.uk"
FTP_DIR = "/pub/databases/opentargets/platform/24.09/output/etl/json/molecule/"
LOCAL_DIR = "data/202409XX/molecule"  # Change this to your desired local directory

download_json_files(FTP_HOST, FTP_DIR, LOCAL_DIR)
