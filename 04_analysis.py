"""_summary_
This script filters the matrix to only include the reference perturbagen (trazodone) 
and the perturbagens that are connected to it. The resulting matrix is saved to a file.
in effect, this script can be skipped if we are to calculate the true jaccard similarity
"""
import os
import pandas as pd
from io import StringIO

if 1:
    
    # timestamp
    import datetime
    now = datetime.datetime.now()
    print("Finished at: ", now.strftime("%Y-%m-%d %H:%M:%S"))
    exit()  # skip this script for now

path_i = "./analysis_level_1/"
path_o = "./analysis_level_1/"

# source
perturbagen_s = "trazodone"

# read the matrix
print("Reading matrix...")
matrix = pd.read_csv(path_i + "matrix.tsv", sep="\t", index_col=0)
print("Done.")

# get the column names for the rows with the source perturbagen

reference = matrix.loc[perturbagen_s,:]

# select the header names for the columns with the value 1
# these are the parameters for comparison

reference = reference[reference == 1].index

print(reference)


# filter only reference columns from the matrix
matrix_o = matrix[reference]

# delete all rows with all zeros
matrix_o = matrix_o[(matrix_o.T != 0).any()]


# save the matrix to a file
print("Saving matrix...")
matrix_o.to_csv(path_o + "matrix_o.tsv", sep="\t")
print("Done.")

import datetime
now = datetime.datetime.now()
print("Finished at: ", now.strftime("%Y-%m-%d %H:%M:%S"))
