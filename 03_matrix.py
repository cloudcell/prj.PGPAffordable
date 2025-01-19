
import os
import pandas as pd
from io import StringIO
from tqdm import tqdm


path_i = "./data/"
path_o = "./analysis_level_1/"

# get a list of files with size greater than 0, exclude directories
# file_list = [f for f in os.listdir(path_i) if os.path.isfile(os.path.join(path_i, f)) and os.path.getsize(os.path.join(path_i, f)) > 0]  # this includes the file test-trazodone.tsv
file_list = [f for f in os.listdir(path_i) if os.path.isfile(os.path.join(path_i, f)) and os.path.getsize(os.path.join(path_i, f)) > 0 and f != "reference.tsv"]


# print the list of files
print(file_list)

# merge all records into one data frame and create an occurrence matrix from the second column

print(pd.__version__)

df = pd.DataFrame()
for file in file_list:
    # use utf-8 encoding to avoid errors
    print(path_i + file)
    
    with open(path_i + file, 'r', encoding='utf-8', errors='replace') as f:
        content = f.read()

    df_tmp = pd.read_csv(StringIO(content), sep="\t", header=None)

    # df_tmp = pd.read_csv(path_i + file, sep="\t", header=None, encoding='utf-8', errors='replace')
    df = pd.concat([df, df_tmp], ignore_index=True)

    
print(df)

# delete all columns except the first and the second
df = df.iloc[:,0:2]

print(df)

# create a matrix with the number of occurrences of node in the second column
# the matrix is symmetric
print("Creating matrix...")
matrix = pd.crosstab(df[0], df[1])
print("Done.")

print(matrix)

# save the matrix to a file
if 0:
    print("Saving matrix...")
    matrix.to_csv(path_o + "matrix.tsv", sep="\t")
    print("Done.")

print("===============================")

# sum the column values
matrix_col_sums = matrix.sum(axis=0)

print(matrix_col_sums)

# sort matrix_col_sums in descending order
matrix_col_sums_sorted = matrix_col_sums.sort_values(ascending=False)

print(matrix_col_sums_sorted)

# save the matrix_col_sums_sorted to a file

matrix_col_sums_sorted.to_csv(path_o + "matrix_col_sums_sorted.tsv", sep="\t", header=False)

# plot the matrix_col_sums_sorted as a bar chart
import matplotlib.pyplot as plt
plt.figure(figsize=(20,10))
plt.bar(matrix_col_sums_sorted[0:100].index, matrix_col_sums_sorted[0:100], color='green')
plt.xticks(rotation=90)
plt.savefig(path_o + "matrix_col_sums_sorted.png")
plt.close()

# -----------------------

path_i = "./analysis_level_1/"
path_o = "./analysis_level_1/"

# source
perturbagen_s = "trazodone"

# # read the matrix
# # matrix = pd.read_csv(path_i + "matrix_o.tsv", sep="\t", index_col=0)
# print("Reading matrix...")
# matrix = pd.read_csv(path_i + "matrix.tsv", sep="\t", index_col=0)
# print("Done.")

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
    jaccard.iloc[i,0] = intersection.sum() / union.sum()  # TODO: check the correctness

# sort the jaccard dataframe in descending order
jaccard = jaccard.sort_values(by="jaccard", ascending=False)

print(jaccard)

# save the matrix to a file
jaccard.to_csv(path_o + "jaccard.tsv", sep="\t")

# timestamp
import datetime
now = datetime.datetime.now()
print("Finished at: ", now.strftime("%Y-%m-%d %H:%M:%S"))
