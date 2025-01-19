"""_summary_
This script gets individual document-level links from the graph
and saves them to files. 
"""
import os
import pandas as pd
from io import StringIO
from tqdm import tqdm

import somarequests as sr

path_and_filename_meta_i = "./metadata/perturbagens_o.txt"
path_and_filename_index_o = "./metadata/perturbagens_soma_names.txt"
path_and_filename_analysis_i = "./analysis_level_1/jaccard.tsv"

path_i = "./data/"
path_o = "./analysis_level_2/"

path_and_filename_ref = path_i + 'reference.tsv'

# source
perturbagen_s = "trazodone"

# create the index file with all the perturbagens from the data folder with the file names beginning with perturbagen_web_
file_list_to_index = [f for f in os.listdir(path_i) if os.path.isfile(os.path.join(path_i, f)) and os.path.getsize(os.path.join(path_i, f)) > 0 and f.startswith("perturbagen_web_")]
# file_list_to_index = [f for f in os.listdir(path_i) if os.path.isfile(os.path.join(path_i, f)) and os.path.getsize(os.path.join(path_i, f)) > 0]


# if the index file exists, delete it
if os.path.isfile(path_and_filename_index_o):
    os.remove(path_and_filename_index_o)

for file in tqdm(file_list_to_index):
    # read the first record from the file and extract the first field
    with open(path_i + file, 'r', encoding='utf-8', errors='replace') as f:
        content = f.read()
    perturbagen_soma_named = content.split("\t")[0]
    # save the perturbagen name to the index file
    with open(path_and_filename_index_o, 'a') as f:
        f.write(perturbagen_soma_named + "\t" + file + "\n")


# read the first record from the analysis file
print("Reading analysis file...")
analysis = pd.read_csv(path_and_filename_analysis_i, sep="\t", index_col=0)
print("Done.")


# read the metadata file
print("Reading metadata file...")
# metadata = pd.read_csv(path_and_filename_meta_i, sep="\t", header=None)
# get the metadata file with the perturbagen names from path_and_filename_index_o
metadata = pd.read_csv(path_and_filename_index_o, sep="\t", header=None)
print("Done.")


for i in tqdm(range(len(analysis))):

    # the first record is the reference node
    if i == 0:
        get_ref_data = True
    else:
        get_ref_data = False

    # get the row name of the first record
    analysis_rec = analysis.iloc[i,:].name
    print(analysis_rec)

    # find the row id in the metadata file having converted the metadata field to lower case
    # row_id = metadata[metadata[0].str.lower() == analysis_rec.lower()].index[0]
    # extract the file name from the metadata file name
    file_name = metadata[metadata[0].str.lower() == analysis_rec.lower()].iloc[0,1]
    # extract the row id from the file name
    row_id_str = int(file_name.split("_")[2].split(".")[0])
    # convert row_id to integer
    row_id = int(row_id_str)
    print(row_id)
    
    # if get_ref_data:
    #     path_and_filename_o = path_o + "ray_df_ref.tsv"
    # else:
    path_and_filename_o = path_o + "ray_df_" + str(row_id).zfill(5) + ".tsv"
    
    if os.path.isfile(path_and_filename_o):
        print("File exists: ", path_and_filename_o)
        continue
    
    if get_ref_data:
        path_and_filename_perturbagen_i = path_and_filename_ref
    else:
        # read the data file with the linked nodes with 5 digits
        path_and_filename_perturbagen_i = path_i + "perturbagen_web_" + str(row_id).zfill(5) + ".tsv"
    

    # if 0:
    #     sr.get_star(concept_name=analysis_rec)
   
    with open(path_and_filename_perturbagen_i, 'r', encoding='utf-8', errors='replace') as f:
        content = f.read()
    df = pd.read_csv(StringIO(content), sep="\t", header=None)


    # df = pd.read_csv(path_and_filename_perturbagen_i, sep="\t", header=None, encoding='utf-8')

    # select the first two columns

    df = df.iloc[:,0:2]
    print(df)


    ray_df = sr.get_rays(pairs=df)

    print(ray_df)

    # save the ray_df to a file
    print("Saving ray_df...")
    ray_df.to_csv(path_and_filename_o, sep="\t")
    print("Done.")




# timestamp
import datetime
now = datetime.datetime.now()
print("Finished at: ", now.strftime("%Y-%m-%d %H:%M:%S"))

