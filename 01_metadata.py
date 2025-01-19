
import pandas as pd

path_and_filename_i = "./metadata/repurposing_samples_20200324.txt"
path_and_filename_o = "./metadata/perturbagens_o.txt"

# read in the data skipping the first 9 rows of metadata

df = pd.read_csv(path_and_filename_i, sep="\t", skiprows=9)

print(df)

# print all data with all the columns 

pd.set_option('display.max_columns', None)

print(df)


# Within the CMap context, "pert_iname" typically refers to the 
# "perturbagen name." A "perturbagen" is any agent that modifies 
# cellular state. In the CMap project, this would usually be a 
# compound (drug or other chemical), a gene expression knockdown 
# or overexpression (using shRNA, CRISPR, etc.), or similar agents.

perturbagen = df["pert_iname"]

# count the total number of perturbagens
print(len(perturbagen))

# count the number of unique perturbagens
print(len(perturbagen.unique()))

# save the unique perturbagens to a file, one line per perturbagen
count = 0
with open(path_and_filename_o, 'w') as f:
    for item in perturbagen.unique():
        f.write("%s\n" % item)
        count += 1
        
print("Number of unique perturbagens: ", count)


        
        