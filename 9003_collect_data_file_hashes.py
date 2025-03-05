import os
import re

def collect_md5_hashes(root_folder, output_file):
    md5_data = []
    
    for dirpath, _, filenames in os.walk(root_folder):
        for file in filenames:
            if file.endswith(".md5"):
                # checking file 
                print(f"Checking file: {file}")
                md5_path = os.path.join(dirpath, file)
                try:
                    with open(md5_path, "r") as f:
                        content = f.read().strip()
                        # match = re.match(r"([a-fA-F0-9]{32})\s+(.*)", content)
                        if content:
                            # hashsum, original_file = match.groups()
                            # strip .md5 extension
                            original_file = file[:-4]
                            md5_data.append((os.path.join(dirpath, original_file), content))
                            print(f"MD5 hash found for {original_file}: {content}")
                except Exception as e:
                    print(f"Error reading {md5_path}: {e}")
    
    md5_data.sort()
    with open(output_file, "w") as out_f:
        out_f.write("Filename\tHashsum\n")
        for filename, hashsum in md5_data:
            out_f.write(f"{filename}\t{hashsum}\n")
    
    print(f"MD5 hashes collected and saved to {output_file}")

if __name__ == "__main__":
    root_folder = "./data/202409XX/"
    output_file = "md5_hashes.tsv"
    collect_md5_hashes(root_folder, output_file)
