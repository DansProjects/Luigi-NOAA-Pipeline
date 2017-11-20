# Luigi-NOAA-Pipeline

Tech: Python, Luigi (pipeline), Pandas (data analysis / summation)  

Data source:  https://www1.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/

#### Pipeline

1. Go to the data source, download all StormEvents_details into the ```results/scraped``` directory
2. Unzip (gzip) the .csv.gz files into .csv and place them into the ```results/extracted``` directory
3. Combine all the .csv files into one file ```results/combined.csv```
4. Load ```results/combined.csv``` into Pandas and calculate desired metrics


#### Usage

Download this code and go to this directory. 

##### With Docker
```
docker build -t luigi-noaa .
docker run -ti luigi-noaa
python3 pipeline.py
```

##### Without Docker
```
pip3 install -r requirements.txt
python3 pipeline.py
```