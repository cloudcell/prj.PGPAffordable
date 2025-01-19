"""_summary_
This script calculates the jaccard similarity for each row in the 
matrix with the source perturbagen (trazodone).

In simple terms: Jaccard Similarity is the number of objects 
the two sets have in common divided by the total number of objects
"""
import os
import pandas as pd
from io import StringIO
from tqdm import tqdm

if 1:
    print("No need in running this script... exiting...")
    exit()

path_i = "./analysis/"
path_o = "./analysis/"

# source
perturbagen_s = "trazodone"

# read the matrix
# matrix = pd.read_csv(path_i + "matrix_o.tsv", sep="\t", index_col=0)
print("Reading matrix...")
matrix = pd.read_csv(path_i + "matrix.tsv", sep="\t", index_col=0)
print("Done.")

# calculate jaccard similarity for each row with the source perturbagen perturbagen_s

ref_row = matrix.loc[perturbagen_s,:]

# for each row in the matrix calculate the jaccard similarity by dividing the intersection of the row with the reference row by the union of the row with the reference row

# create a dataframe to store the jaccard similarities with names of the rows
jaccard = pd.DataFrame(index=matrix.index, columns=["jaccard"])

# number of rows in the matrix


for i in tqdm(range(len(matrix))):
    # get the row
    row = matrix.iloc[i,:]
    # calculate the intersection
    intersection = (ref_row == row) & (ref_row == 1)
    # calculate the union
    union = (ref_row == 1) | (row == 1)
    # calculate the jaccard similarity
    jaccard.iloc[i,0] = intersection.sum() / union.sum()

# sort the jaccard dataframe in descending order
jaccard = jaccard.sort_values(by="jaccard", ascending=False)

print(jaccard)

# save the matrix to a file
jaccard.to_csv(path_o + "jaccard.tsv", sep="\t")

# timestamp
import datetime
now = datetime.datetime.now()
print("Finished at: ", now.strftime("%Y-%m-%d %H:%M:%S"))

