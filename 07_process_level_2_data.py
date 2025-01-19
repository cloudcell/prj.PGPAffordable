"""_summary_
This script calculates cosine similarity for each pair
comprised of the source perturbagen (trazodone) and the perturbagens 
"""
import os
import glob
import pandas as pd
from io import StringIO
from tqdm import tqdm
import ast
import numpy as np

DEBUG_MODE = False

path_i = "./analysis_level_2/"
path_o = "./analysis_level_3/"
path_o2 = "./analysis_level_4/"

perturbagen_ref = "trazodone"
# perturbagen_file_ref = "ray_df_ref.tsv"
perturbagen_file_ref = "ray_df_06331.tsv"

# read the reference file
print("Reading reference file...")
reference = pd.read_csv(path_i + perturbagen_file_ref, sep="\t", index_col=0)
print("Done.")


# List files that conform to the pattern "ray_df_?????.tsv"
list_of_rays = glob.glob(os.path.join(path_i, "ray_df_?????.tsv"))

if DEBUG_MODE:
    print(list_of_rays)

# create an empty dataframe to store the results the size of the length of the list of rays
similarity = pd.DataFrame(columns=["perturbagen", "distance", "similarity"], index=range(len(list_of_rays)))

def datadump_unpack(datadump, verbose=False):
    """_summary_
    This function unpacks the datadump field from the ray dataframe
    """
    # take the datadump field and unpack json list into a separate dataframe
    # example: [['"Raman and 19F(1H) nuclear Overhauser evidence for a rigid solution conformation of Escherichia coli 5-fluorouracil 5S ribonucleic acid. {Filename: pubmed.abstract.6162474.txt.array.txt.gpted.tsv} {Link: https://pubmed.ncbi.nlm.nih.gov/6162474/}"', 'Associative', 'pos']]
    if verbose:
        print("============================================----RAW----============================================")
        print(datadump)
    
    target_datadump_list = ast.literal_eval(datadump)
    
    # create an empty dataframe to store the results
    target_datadump_df = pd.DataFrame(columns=["description", "relation", "correlation", "flg_cau", "num_cau", "flg_ass", "num_ass", "num_agg"], index=range(len(target_datadump_list)))
    
    # process all lists in the list
    for j in range(len(target_datadump_list)):
        # # remove the first and the last characters from the string
        # target_datadump_list[j] = target_datadump_list[j][1:-1]
        
        inner_list = target_datadump_list[j]

        # Extract the components
        description = inner_list[0]
        relation = inner_list[1]
        correlation = inner_list[2]
        
        target_datadump_df.iloc[j,0] = description
        target_datadump_df.iloc[j,1] = relation
        target_datadump_df.iloc[j,2] = correlation
        target_datadump_df.iloc[j,3] = (relation == "Causal") * 1.0
        if relation == "Causal":
            target_datadump_df.iloc[j,4] = (correlation == "pos") * 1.0 - (correlation == "neg") * 1.0
        else:
            target_datadump_df.iloc[j,4] = 0.0
        target_datadump_df.iloc[j,5] = (relation == "Associative") * 1.0
        if relation == "Associative":
            target_datadump_df.iloc[j,6] = (correlation == "pos") * 1.0 - (correlation == "neg") * 1.0
        else:
            target_datadump_df.iloc[j,6] = 0.0
        target_datadump_df.iloc[j,7] = (correlation == "pos") * 1.0 - (correlation == "neg") * 1.0
                
        if verbose:
            print("=========BEGIN============")
            print("Description:", description)
            print("Relation:", relation)
            print("Correlation:", correlation)
            print("========= END ============")

    return target_datadump_df


def data_aggregate_tiny(dataframe, metadata: str, verbose=False):
    """_summary_
    This function aggregates the datadump dataframe
    """
    # assign the number of rows in a dataframe to a variable using shape as float
    num_rows = float(dataframe.shape[0])
    if num_rows == 0:
        # take the first field of the data
        # write a message to the log file
        with(open('error.log', 'w', encoding="utf-8")) as f:
            message = "<-- the dataframe is empty."
            f.write(metadata + "\t" + message)
        return 0.0
    return float(dataframe["num_agg"].sum()/num_rows)


def calculate_euclidean_distance(reference: pd.DataFrame, target: pd.DataFrame, verbose=False):
    
    # select "node2" and "aggregate" columns from the reference and target dataframes
    reference_selected = reference[["node2", "aggregate"]].copy()
    # convert the node2 column to string
    reference_selected["node2"] = reference_selected["node2"].astype(str)
    
    # rename the "aggregate" column to "aggregate_ref"
    reference_selected = reference_selected.rename(columns={"aggregate": "aggregate_ref"})
    
    
    target_selected = target[["node2", "aggregate"]].copy()
    # convert the node2 column to string
    target_selected["node2"] = target_selected["node2"].astype(str)
    
    # rename the "aggregate" column to "aggregate_tgt"
    target_selected = target_selected.rename(columns={"aggregate": "aggregate_tgt"})
    
    # join the reference and target dataframes on the "node2" column, filling NA with 0
    computation = pd.merge(reference_selected, target_selected, on="node2", how="outer").fillna(0.0)
    
    # print(computation)
    
    # compute the Euclidean distance
    distance = np.sqrt(np.sum((computation["aggregate_tgt"] - computation["aggregate_ref"]) ** 2))
    
    # if verbose:
    #     print(f"Euclidean distance: {distance}")

    return distance


def calculate_cosine_similarity(reference: pd.DataFrame, target: pd.DataFrame, verbose=False):
    
    # select "node2" and "aggregate" columns from the reference and target dataframes
    reference_selected = reference[["node2", "aggregate"]].copy()
    # convert the node2 column to string
    reference_selected["node2"] = reference_selected["node2"].astype(str)
    
    # rename the "aggregate" column to "aggregate_ref"
    reference_selected = reference_selected.rename(columns={"aggregate": "aggregate_ref"})
    
    
    target_selected = target[["node2", "aggregate"]].copy()
    # convert the node2 column to string
    target_selected["node2"] = target_selected["node2"].astype(str)
    
    # rename the "aggregate" column to "aggregate_tgt"
    target_selected = target_selected.rename(columns={"aggregate": "aggregate_tgt"})
    
    # join the reference and target dataframes on the "node2" column, filling NA with 0
    computation = pd.merge(reference_selected, target_selected, on="node2", how="outer").fillna(0.0)
    
    try:
        divisor = (np.sqrt(np.sum(computation["aggregate_tgt"] ** 2)) * np.sqrt(np.sum(computation["aggregate_ref"] ** 2)))
        if divisor == 0.0:
            similarity = 0.0
        else:
            similarity = np.dot(computation["aggregate_tgt"], computation["aggregate_ref"]) / divisor
    except ZeroDivisionError:
        with(open('error.log', 'w', encoding="utf-8")) as f:
            message = "<-- division by zero."
            f.write(reference["node2"] +  "\t" + message)

    return similarity


reference["aggregate"] = 0.0 

for i in range(len(reference)):

    # take the datadump field and unpack json list into a separate dataframe
    # example: [['"Raman and 19F(1H) nuclear Overhauser evidence for a rigid solution conformation of Escherichia coli 5-fluorouracil 5S ribonucleic acid. {Filename: pubmed.abstract.6162474.txt.array.txt.gpted.tsv} {Link: https://pubmed.ncbi.nlm.nih.gov/6162474/}"', 'Associative', 'pos']]

    df = datadump_unpack(datadump=reference["datadump"][i])
    # print(df)
    
    aggregate = data_aggregate_tiny(df, metadata=reference["node1"][i] + "\t" + reference["node2"][i])
    # print(aggregate)
    
    # add a column "aggregate" to the target dataframe using iloc
    reference.iloc[i, 3] = aggregate
    # target[i, "aggregate"] = aggregate

    # reference = datadump_unpack(datadump=reference["datadump"][0])
    # reference_agg = data_aggregate_tiny(reference)


# ray_id = 0
for ray_id in tqdm(range(len(list_of_rays))):
    
    ray_file = list_of_rays[ray_id]
    
    # delete the folder path from the name either using windows or linux conventions and then extract the number from the file name
    original_ray_id = int(os.path.splitext(os.path.basename(ray_file))[0].split("_")[2])    
    
    target = pd.read_csv(ray_file, sep="\t", index_col=0)
    target_name = target.iloc[0,0]
    
    # open file in path_o2 without the with statement
    f = open(path_o2 + "ray_df_analytics_" + str(original_ray_id).zfill(5) + ".tsv", "w", encoding="utf-8", newline="\n")
    
    
    target["aggregate"] = 0.0
    
    for i in range(len(target)):

        # take the datadump field and unpack json list into a separate dataframe
        # example: [['"Raman and 19F(1H) nuclear Overhauser evidence for a rigid solution conformation of Escherichia coli 5-fluorouracil 5S ribonucleic acid. {Filename: pubmed.abstract.6162474.txt.array.txt.gpted.tsv} {Link: https://pubmed.ncbi.nlm.nih.gov/6162474/}"', 'Associative', 'pos']]
        # print(i)
        df = datadump_unpack(datadump=target["datadump"][i])
        # print(df)
        
        df_for_dumping_to_analytics = df.copy()
        df_for_dumping_to_analytics["node1"] = str(target["node1"][i])
        df_for_dumping_to_analytics["node2"] = str(target["node2"][i])
        
        # rearrange the columns in the dataframe as node1 node2 relation correlation flg_cau num_cau flg_ass num_ass num_agg description
        columns_order = ["node1", "node2", "relation", "correlation", "flg_cau", "num_cau", "flg_ass", "num_ass", "num_agg", "description"]
        df_for_dumping_to_analytics = df_for_dumping_to_analytics[columns_order]
        
        if i == 0:
            # write the header
            f.write(df_for_dumping_to_analytics.to_csv(sep="\t", index=False, header=True, encoding="utf-8"))
        else:
            f.write(df_for_dumping_to_analytics.to_csv(sep="\t", index=False, header=False, encoding="utf-8"))
        
        aggregate = data_aggregate_tiny(df, metadata= str(target["node1"][i]) + "\t" + str(target["node2"][i]))
        # print(aggregate)
        
        # add a column "aggregate" to the target dataframe using iloc
        target.iloc[i, 3] = aggregate
        # target[i, "aggregate"] = aggregate
        
    f.close()
    
    if DEBUG_MODE:    
        print(target)  # with the aggregate column added
    
    # print(type(reference_agg))
    # print(reference_agg)
    
    # Convert reference_agg and target to DataFrames
    reference_df = pd.DataFrame(reference)
    target_df = pd.DataFrame(target)

    similarity.iloc[ray_id,0] = target_name
    similarity.iloc[ray_id,1] = calculate_euclidean_distance(reference=reference_df, target=target_df)
    similarity.iloc[ray_id,2] = calculate_cosine_similarity(reference=reference_df, target=target_df)
    
    # ray_id += 1


print(similarity)


similarity_sorted = similarity.sort_values(by="similarity", ascending=False)


if 1:
    # save the dataframe to a file
    similarity_sorted.to_csv(path_o + "similarity.tsv", sep="\t")



# timestamp
import datetime
now = datetime.datetime.now()
print("Finished at: ", now.strftime("%Y-%m-%d %H:%M:%S"))

