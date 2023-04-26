
import pandas as pd
import dask.dataframe as dd
import warnings
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
warnings.simplefilter(action='ignore', category=FutureWarning)

print('Loading data...')
# Load data into dataframes
trees = pd.read_csv('./data/trees.csv')
temps = pd.read_csv('./data/temps.csv')

dtypes = {
	'Sensor.ID': str,
	'AirTemp': float,
	'Latitude': float,
	'Longitude': float,
	'Day': 'datetime64[ns]',
	'Hour': int,
	'Year': int,
	'Install.Type': str,
	'Borough': str,
	'ntacode': str,
}

temps = temps.astype(dtypes)

dtypes = {
	'created_at': 'datetime64[ns]',
	'tree_id': 'int32',
	'block_id': 'int32',
	'tree_dbh': 'int32',
	'stump_diam': 'int32',
	'curb_loc': 'str',
	'status': 'str',
	'health': 'str',
	'spc_latin': 'str',
	'spc_common': 'str',
	'steward': 'str',
	'guards': 'str',
	'sidewalk': 'str',
	'user_type': 'str',
	'problems': 'str',
	'root_stone': 'str',
	'root_grate': 'str',
	'root_other': 'str',
	'trunk_wire': 'str',
	'trnk_light': 'str',
	'trnk_other': 'str',
	'brch_light': 'str',
	'brch_shoe': 'str',
	'brch_other': 'str',
	'address': 'str',
	'postcode': 'int32',
	'zip_city': 'str',
	'community board': 'int32',
	'borocode': 'int32',
	'borough': 'str',
	'cncldist': 'int32',
	'st_assem': 'int32',
	'st_senate': 'int32',
	'nta': 'str',
	'nta_name': 'str',
	'boro_ct': 'int32',
	'state': 'str',
	'latitude': 'float32',
	'longitude': 'float32',
	'x_sp': 'float32',
	'y_sp': 'float32',
	'census tract': 'float32',
	'bin': 'float32',
	'bbl': 'float32',
}

trees = trees.astype(dtypes)

# Drop columns that are not relevant to the analysis. This includes:
# - spatial columns (since we are not mapping using GIS)
# - columns that contain redundant information (e.g. borocode encodes same info as borough)
# - columns that are too granular (e.g. bin) and too broad (e.g. state) to be useful
cols_to_drop = [
	'block_id',
	'x_sp',
	'y_sp',
	'zip_city',
	'census tract',
	'borocode',
	'boro_ct',
	'nta_name',
	'cncldist',
	'st_assem',
	'st_senate',
	'community board',
	'council district',
	'census tract',
	'bin',
	'bbl',
	'state'
]

[trees.drop(columns=col, inplace=True) for col in cols_to_drop if col in trees.columns]

# Sampling the temp data
# Keeping only hour 0, 6, 12, 18 for each day for each location
# daily_avg_temps = temps[temps['Hour'].isin([0, 6, 12, 18])]
aggregation_functions = {
						'AirTemp': 'mean',
						'Hour': 'first',
						'Latitude': 'first',
						'Longitude': 'first',
						'Year': 'first',
						'Install.Type': 'first',
						'Borough': 'first',
						'ntacode': 'first'
					}
daily_avg_temps = temps.groupby(['Sensor.ID', 'Day']).agg(aggregation_functions).reset_index()

# Dropping columns that are not relevant to the analysis.
cols_to_drop = ['Year']
[daily_avg_temps.drop(columns=col, inplace=True) for col in cols_to_drop if col in trees.columns]

# Rename column to match trees dataset for joining.
if 'ntacode' in daily_avg_temps.columns:
	daily_avg_temps['nta'] = daily_avg_temps['ntacode']
	daily_avg_temps.drop(columns=['ntacode'], inplace=True)

# Trees has more NTA codes than temps, so we will filter temps to only include the NTA codes that are relevant to trees.
relevant_nta_codes = daily_avg_temps['nta'].unique()
trees_nta_filtered = trees[trees['nta'].isin(relevant_nta_codes)]

# Filter out trees that are stumps
trees_nta_filtered = trees_nta_filtered[trees_nta_filtered['stump_diam'] == 0]
trees_nta_filtered.drop(columns=['stump_diam'], inplace=True)

# Randomly sample trees so there are 91696 -> 50000 rows.
trees_nta_filtered = trees_nta_filtered.sample(n=50000, random_state=42)

cols_to_drop = [
	'root_stone',
	'root_grate',
	'root_other',
	'trunk_wire',
	'trnk_light',
	'trnk_other',
	'brch_light',
	'brch_shoe',
	'brch_other',
	'borough',
	'created_at',
	'tree_id'
]
trees_nta_filtered.drop(columns=cols_to_drop, inplace=True)

# To make the join faster, index on the merging columns and sort the dataframes by the index.
daily_avg_temps.set_index('nta', inplace=True)
trees_nta_filtered.set_index('nta', inplace=True)

# Further filter temps to only include every third day.
daily_avg_temps_filtered = daily_avg_temps[daily_avg_temps['Day'].dt.day % 7 == 0]


print("Merging dataframes...")
# Use dask to merge the dataframes in parallel.
dd_daily_avg_temps = dd.from_pandas(daily_avg_temps_filtered, npartitions=3)
dd_trees_nta_filtered = dd.from_pandas(trees_nta_filtered, npartitions=3)
integrated = dd.merge(dd_daily_avg_temps, dd_trees_nta_filtered, on='nta').compute()

cols_to_drop = [
	'address',
	'latitude',
	'longitude',
	'Latitude',
	'Longitude',
	'steward',
	'Hour',
	'guards',
	'Day',
	'Sensor.ID'
]

integrated.drop(columns=cols_to_drop, inplace=True)

print("Cleaning outputs + renaming columns...")
# Sample integrated dataset from 20M rows to 5k rows.
# We need to sample the data because the dataset is too large to read.
integrated = integrated.sample(n=50000, random_state=42)

# Separate temperatures so 5% of the data is used for each bin.
integrated['AirTempBinned'] = pd.qcut(integrated['AirTemp'], q=5)

integrated['AirTempBinned'] = integrated['AirTempBinned'].astype('str')
integrated['AirTempBinned'] = integrated['AirTempBinned'].str.replace('\(71.353, 73.473]','AirTemp: (71.353, 73.473]')
integrated['AirTempBinned'] = integrated['AirTempBinned'].str.replace('\(73.473, 76.336]','AirTemp: (73.473, 76.336]')
integrated['AirTempBinned'] = integrated['AirTempBinned'].str.replace('\(76.336, 82.603]','AirTemp: (76.336, 82.603]')
integrated['AirTempBinned'] = integrated['AirTempBinned'].str.replace('\(73.473, 76.336]','AirTemp: (73.473, 76.336]')
integrated['AirTempBinned'] = integrated['AirTempBinned'].str.replace('\(52.443000000000005, 71.353]','AirTemp: (52.443000000000005, 71.353]')

integrated['TreeDBHBinned'] = pd.qcut(integrated['tree_dbh'] , q=5)

integrated['TreeDBHBinned'] = integrated['TreeDBHBinned'].astype('str')
integrated['TreeDBHBinned'] = integrated['TreeDBHBinned'].str.replace('\(-0.001, 4.0]','Tree Diameter: (-0.001, 4.0]')
integrated['TreeDBHBinned'] = integrated['TreeDBHBinned'].str.replace('\(4.0, 6.0]','Tree Diameter: (4.0, 6.0]')
integrated['TreeDBHBinned'] = integrated['TreeDBHBinned'].str.replace('\(6.0, 9.0]','Tree Diameter: (6.0, 9.0]')
integrated['TreeDBHBinned'] = integrated['TreeDBHBinned'].str.replace('\(9.0, 14.0]','Tree Diameter: (9.0, 16.0]')
integrated['TreeDBHBinned'] = integrated['TreeDBHBinned'].str.replace('\(16.0, 59.0]','Tree Diameter: (16.0, 59.0]')

integrated.drop(columns=['AirTemp', 'tree_dbh'], inplace=True)

integrated['status'] = integrated['status'].str.replace('Alive','Status: Alive')
integrated['status'] = integrated['status'].str.replace('Dead','Status: Dead')

# In integrated['problems'] replace 'None' with 'No Problems'
integrated['problems'] = integrated['problems'].str.replace('None','Problems: None')

integrated['health'] = integrated['health'].str.replace('Good','Health: Good')
integrated['health'] = integrated['health'].str.replace('Fair','Health: Fair')
integrated['health'] = integrated['health'].str.replace('Poor','Health: Poor')
integrated['health'] = integrated['health'].str.replace('None','Health: Not Available')

integrated['sidewalk'] = integrated['sidewalk'].str.replace('NoDamage','Sidewalk: No Damage')
integrated['sidewalk'] = integrated['sidewalk'].str.replace('Damage','Sidewalk: Damage')

integrated['user_type'].value_counts()
integrated['user_type'] = integrated['user_type'].str.replace('TreesCount Staff','Surveyor Type: TreesCount Staff')
integrated['user_type'] = integrated['user_type'].str.replace('Volunteer','Surveyor Type: Volunteer')
integrated['user_type'] = integrated['user_type'].str.replace('NYC Parks Staff','Surveyor Type: NYC Parks Staff')

print("Writing to csv...")
integrated.to_csv('INTEGRATED-DATASET.csv')
