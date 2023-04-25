#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import dask.dataframe as dd
import wget

pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 1000)

# Load data into dataframes
# trees_url = "http://data.cityofnewyork.us/api/views/uvpi-gqnh/rows.csv?accessType=DOWNLOAD"
# temps_url = "http://data.cityofnewyork.us/api/views/qdq3-9eqn/rows.csv?accessType=DOWNLOAD"

# print(f"Downloading trees dataset...")
# wget.download(trees_url, '../data/trees.csv')
# print(f"Downloading temps dataset...")
# wget.download(temps_url, '../data/temps.csv')

print(f"Reading datasets from csv...")
trees = pd.read_csv("/Users/yamini/nyc-data-mining/data/2015_Street_Tree_Census_-_Tree_Data.csv")
temps = pd.read_csv("/Users/yamini/nyc-data-mining/data/Hyperlocal_Temperature_Monitoring.csv")

###  Convert Datatypes for all columns in `temps` and `trees` to reduce memory usage and enable comparisons
print(f"Cleaning datasets... ")

dtypes = {
    "Sensor.ID": str,
    "AirTemp": float,
    "Latitude": float,
    "Longitude": float,
    "Day": "datetime64[ns]",
    "Hour": int,
    "Year": int,
    "Install.Type": str,
    "Borough": str,
    "ntacode": str,
}

temps = temps.astype(dtypes)
trees.columns
dtypes = {
    "created_at": "datetime64[ns]",
    "tree_id": "int32",
    "block_id": "int32",
    "tree_dbh": "int32",
    "stump_diam": "int32",
    "curb_loc": "str",
    "status": "str",
    "health": "str",
    "spc_latin": "str",
    "spc_common": "str",
    "steward": "str",
    "guards": "str",
    "sidewalk": "str",
    "user_type": "str",
    "problems": "str",
    "root_stone": "str",
    "root_grate": "str",
    "root_other": "str",
    "trunk_wire": "str",
    "trnk_light": "str",
    "trnk_other": "str",
    "brch_light": "str",
    "brch_shoe": "str",
    "brch_other": "str",
    "address": "str",
    "postcode": "int32",
    "zip_city": "str",
    "community board": "int32",
    "borocode": "int32",
    "borough": "str",
    "cncldist": "int32",
    "st_assem": "int32",
    "st_senate": "int32",
    "nta": "str",
    "nta_name": "str",
    "boro_ct": "int32",
    "state": "str",
    "latitude": "float32",
    "longitude": "float32",
    "x_sp": "float32",
    "y_sp": "float32",
    "census tract": "float32",
    "bin": "float32",
    "bbl": "float32",
}

trees = trees.astype(dtypes)
trees.head()


### Filter columns of datasets before merging
# Drop columns that are not relevant to the analysis. This includes:
# - spatial columns (since we are not mapping using GIS)
# - columns that contain redundant information (e.g. borocode encodes same info as borough)
# - columns that are too granular (e.g. bin) and too broad (e.g. state) to be useful
cols_to_drop = [
    "block_id",
    "x_sp",
    "y_sp",
    "zip_city",
    "census tract",
    "borocode",
    "boro_ct",
    "nta_name",
    "cncldist",
    "st_assem",
    "st_senate",
    "community board",
    "council district",
    "census tract",
    "bin",
    "bbl",
    "state",
]

[trees.drop(columns=col, inplace=True) for col in cols_to_drop if col in trees.columns]
trees.head()

# Sampling the temp data
# Keeping only hour 0, 6, 12, 18 for each day for each location
# daily_avg_temps = temps[temps['Hour'].isin([0, 6, 12, 18])]
aggregation_functions = {
    "AirTemp": "mean",
    "Hour": "first",
    "Latitude": "first",
    "Longitude": "first",
    "Year": "first",
    "Install.Type": "first",
    "Borough": "first",
    "ntacode": "first",
}
daily_avg_temps = (
    temps.groupby(["Sensor.ID", "Day"]).agg(aggregation_functions).reset_index()
)

# Dropping columns that are not relevant to the analysis.
cols_to_drop = ["Year"]
[
    daily_avg_temps.drop(columns=col, inplace=True)
    for col in cols_to_drop
    if col in trees.columns
]
daily_avg_temps.head()

# Rename column to match trees dataset for joining.
if "ntacode" in daily_avg_temps.columns:
    daily_avg_temps["nta"] = daily_avg_temps["ntacode"]
    daily_avg_temps.drop(columns=["ntacode"], inplace=True)

# Trees has more NTA codes than temps, so we will filter temps to only include the NTA codes that are relevant to trees.
relevant_nta_codes = daily_avg_temps["nta"].unique()
trees_nta_filtered = trees[trees["nta"].isin(relevant_nta_codes)]

# Filter out trees that are stumps
trees_nta_filtered = trees_nta_filtered[trees_nta_filtered["stump_diam"] == 0]
trees_nta_filtered.drop(columns=["stump_diam"], inplace=True)

# Randomly sample trees so there are 91696 -> 50000 rows.
trees_nta_filtered = trees_nta_filtered.sample(n=50000, random_state=42)

trees_nta_filtered.head()

cols_to_drop = [
    "root_stone",
    "root_grate",
    "root_other",
    "trunk_wire",
    "trnk_light",
    "trnk_other",
    "brch_light",
    "brch_shoe",
    "brch_other",
    "borough",
    "created_at",
    "tree_id",
]
trees_nta_filtered.drop(columns=cols_to_drop, inplace=True)

# To make the join faster, index on the merging columns and sort the dataframes by the index.
daily_avg_temps.set_index("nta", inplace=True)
trees_nta_filtered.set_index("nta", inplace=True)

# Further filter temps to only include every third day.
daily_avg_temps_filtered = daily_avg_temps[daily_avg_temps["Day"].dt.day % 7 == 0]

# Use dask to merge the dataframes in parallel.
dd_daily_avg_temps = dd.from_pandas(daily_avg_temps_filtered, npartitions=3)
dd_trees_nta_filtered = dd.from_pandas(trees_nta_filtered, npartitions=3)
print(f"Merging datasets...")
integrated = dd.merge(dd_daily_avg_temps, dd_trees_nta_filtered, on="nta").compute()

integrated.head()

cols_to_drop = [
    "address",
    "latitude",
    "longitude",
    "Latitude",
    "Longitude",
    "steward",
    "Hour",
    "guards",
    "Day",
    "Sensor.ID",
]

integrated.drop(columns=cols_to_drop, inplace=True)


integrated.head()


# In integrated['problems'] replace 'None' with 'No Problems'
integrated["problems"] = integrated["problems"].str.replace("None", "NoProblems")

integrated.shape

# Sample integrated dataset from 20M rows to 50k rows.
# We need to sample the data because the dataset is too large to read,
# and running apriori on the entire dataset would require multiple
# passes over the data, which would take too long.
print(f"Sampling dataset down to 5k rows...")
integrated = integrated.sample(n=5000, random_state=42)
integrated.shape

# Separate temperatures so 5% of the data is used for each bin.
integrated["AirTempBinned"] = pd.qcut(integrated["AirTemp"], q=5)
integrated["AirTempBinned"].value_counts()

integrated["TreeDBHBinned"] = pd.qcut(integrated["tree_dbh"], q=5)
integrated["TreeDBHBinned"].value_counts()

integrated.drop(columns=["AirTemp", "tree_dbh"], inplace=True)
integrated.head()

print(f"Sending data to ../INTEGRATED-DATASET.csv")
integrated.to_csv("../INTEGRATED-DATASET.csv")
