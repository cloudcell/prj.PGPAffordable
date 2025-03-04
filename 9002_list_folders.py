from ftplib import FTP

ftp = FTP('ftp.ebi.ac.uk')
ftp.login('anonymous', 'asdf@asdf.com')

files = []
folders = []

def parse_ftp_list(line):
    parts = line.split(maxsplit=8)  # Limit splitting to preserve filenames with spaces
    parts = line.split()
    name = parts[-1]
    
    print(line)
    if line.startswith('d'):
        folders.append(name)
    else:
        files.append(name)

ftp.retrlines('LIST', parse_ftp_list)

print("Folders:", folders)
print("Files:", files)

ftp.quit()

ftp.dir()