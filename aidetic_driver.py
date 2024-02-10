from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from math import *
import folium
import argparse

def initialize_variables():
        parser = argparse.ArgumentParser()
        read_path = parser.add_argument('--read_path',type=str)
        write_path = parser.add_argument('--write_path',type=str)
        read_format= parser.add_argument('--read_format',type=str)
        write_format = parser.add_argument('--write_format',type=str)
        args = parser.parse_args()
        read_path = args.read_path
        write_path = args.write_path
        read_format = args.read_format
        write_format = args.write_format
        return read_path,write_path,read_format,write_format

def extract_data(spark,path,format,header=None):
    if header == 'Y':
        return spark.read.format(format).load(path,header=True)
    else:
        return spark.read.format(format).load(path)

def persist_data(df,path,format,mode):
    df.write.mode(mode).option("header",True).format(format).save(path)

@udf(returnType=StringType())
def categorizeEarthquakes(var):
    if var <= 5.8:
        return str('Low')
    elif var > 5.8 and var <= 6.9:
        return str('Moderate')
    else:
        return str('High')

@udf(returnType=FloatType())
def distance(lat1, lat2, lon1, lon2):     
    lon1 = radians(lon1)
    lon2 = radians(lon2)
    lat1 = radians(lat1)
    lat2 = radians(lat2)
        
    dlon = lon2 - lon1 
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    
    c = 2 * asin(sqrt(a)) 
        
        
    r = 6371
    return(c * r)

def plotDot(latitude,longitude,map,marker_color):
        folium.CircleMarker(location=[latitude, longitude],radius=2,weight=5).add_to(map)

def create_map(df):
    map = folium.Map(prefer_canvas=True)

    for i in df.select('Latitude','Longitude','earthquake_level').collect():
        marker = ''
        if i.__getitem__('earthquake_level') == 'Low':
             marker = 'blue'
        elif i.__getitem__('earthquake_level') == 'Moderate':
             marker = 'yellow'
        else:
             marker = 'red'
        folium.CircleMarker(location=[i.__getitem__('Latitude'), i.__getitem__('Longitude')],radius=2,weight=5,color = marker,fill_color = marker).add_to(map)

    map.fit_bounds(map.get_bounds())
    map.save("map.html")

if __name__ == '__main__':

    read_path,write_path,read_format,write_format = initialize_variables()

    #Creating Spark Session
    spark = SparkSession.builder.getOrCreate() 

    #1.Load the  dataset into a PySpark DataFrame.
    print('1.Load the  dataset into a PySpark DataFrame.')
    df = extract_data(spark,read_path,read_format,'Y')
    df.show()

    #Data Cleaning & 2.Convert the Date and Time columns into a timestamp column named Timestamp.
    #Assumptions Taken :
    #2.1 Dropping rows if any column is having null values
        
    print('2.Convert the Date and Time columns into a timestamp column named Timestamp.')
    df = df.select('Date','Time','Latitude','Longitude','Type','Depth','Magnitude') \
    .na.drop('any') \
    .withColumn('Latitude',df.Latitude.cast('float')) \
    .withColumn('Longitude',df.Longitude.cast('float')) \
    .withColumn('Depth',df.Depth.cast('float')) \
    .withColumn('Magnitude',df.Magnitude.cast('float')) \
    .withColumn('Timestamp',to_timestamp(regexp_replace(concat_ws(' ',df.Date,df.Time),'/','-'),'MM-dd-yyyy HH:mm:ss')) 

    df.show()

    #3.Filter the dataset to include only earthquakes with a magnitude greater than 5.0.
    print('3.Filter the dataset to include only earthquakes with a magnitude greater than 5.0.')
    df = df.filter('Magnitude > 5.0')
    df.show()

    #4.Calculate the average depth and magnitude of earthquakes for each earthquake type.
    print('4.Calculate the average depth and magnitude of earthquakes for each earthquake type.')
    df.groupBy('Type') \
        .agg(avg('Depth').alias('avg_depth'),avg('Magnitude').alias('avg_magnitude')) \
        .show()

    #5.Implement a UDF to categorize the earthquakes into levels (e.g., Low, Moderate, High) based on their magnitudes.

    print('5.Implement a UDF to categorize the earthquakes into levels (e.g., Low, Moderate, High) based on their magnitudes.')

    #Assumptions Taken :
    #5.1 If Magnitude of earthquake is <= 5.8 then I've marked it as 'Low'
    #5.2 If Magnitude > 5.8 and <= 6.9, I've marked it as 'Moderate'
    #5.3 If Magnitude > 6.9, I've marked it as 'High'
        
    df = df.withColumn('earthquake_level',categorizeEarthquakes(col('Magnitude')))
    df.show()

    #6.Calculate the distance of each earthquake from a reference location (e.g., (0, 0)).

    print('6.Calculate the distance of each earthquake from a reference location (e.g., (0, 0)).')

    df = df.withColumn('distance_of_earthquake_kms',distance(col('Latitude'), lit(0), col('Longitude'), lit(0)))
    df.show()

    #7.Visualize the geographical distribution of earthquakes on a world map using appropriate libraries (e.g., Basemap or Folium).

    #Assumptions Taken :
    #5.1 If Magnitude of earthquake is <= 5.8 then I've marked the color in the map as 'blue'
    #5.2 If Magnitude > 5.8 and <= 6.9, I've marked the color in the map as 'yellow'
    #5.3 If Magnitude > 6.9, I've marked the color in the map as 'red'
        
    create_map(df)

    #8.Please include the final csv in the repository.

    persist_data(df,write_path,write_format,'overwrite')
