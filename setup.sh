sudo apt-get update && sudo apt-get upgrade
apt install wget
# Download datasets into data
# echo "Downloading datasets..."
# cd data
# wget http://data.cityofnewyork.us/api/views/qdq3-9eqn/rows.csv?accessType=DOWNLOAD
# mv rows.csv?accessType=DOWNLOAD temps.csv

# wget http://data.cityofnewyork.us/api/views/uvpi-gqnh/rows.csv?accessType=DOWNLOAD
# mv rows.csv?accessType=DOWNLOAD trees.csv

# cd ../
sudo apt-get install python3-pandas
pip3 install pandas
python -m pip install "dask[complete]"
python -m pip install -U prettytable


