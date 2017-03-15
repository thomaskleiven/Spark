#./usr/localspark-2.1.0-bin-hadoop2.7/python/
import sys
sys.path.append("/usr/local/spark/python/")
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import functions as funcs
from pyspark.sql import functions as col
from pyspark.sql.functions import *
import cProfile
import json
import numpy as np
import csv
from multiprocessing import Pool

sc = SparkContext()
sqlCtx = SQLContext(sc)


def getListings():
    listings_rdd = sc.textFile("airbnb_datasets/listings_us.csv").map(lambda x: x.split('\t'))
    head = listings_rdd.filter(lambda line: line[0] == 'access')
    listings_fields=[]

    for name in range(0, len(listings_rdd.take(1)[0])):                         #Create columns to Data Frame
        temp = head.take(1)[0][name]
        listings_fields.append(StructField(temp, StringType(), False))

    listings_rdd = listings_rdd.filter(lambda line: line[0] != 'access')

    listings_schema = StructType(listings_fields)
    return sqlCtx.createDataFrame(listings_rdd, listings_schema)           #Create Data Frame

listings = getListings()
listings = listings.withColumn("price", regexp_replace("price", "\$", ""))
listings = listings.withColumn("price", regexp_replace("price", ",", ""))
slistings = listings.withColumn('price', listings['price'].cast(DoubleType()))
sqlCtx.registerDataFrameAsTable(listings, "listings")

def inner_loop(result):
    xPos = float(result[0])
    yPos = float(result[1])
    for feature in data['features']:
        N = len(feature['geometry']['coordinates'][0][0])
        j = N - 1
        vertx = np.zeros(N)
        verty = np.zeros(N)

        for k in range(0, N):
            vertx[k] = float(feature['geometry']['coordinates'][0][0][k][1])
            verty[k] = float(feature['geometry']['coordinates'][0][0][k][0])
        cross = 0
        for i in range(0,N - 1):
            if(((verty[i] > yPos) != (verty[j]>yPos)) and (xPos < (vertx[j] - vertx[i]) * ((yPos-verty[i])/(verty[j]-verty[i]))+vertx[i])):
                cross += 1
            j = i
        if(cross%2 != 0):
            neighbourhood = [feature['properties']['neighbourhood'], xPos, yPos]
            return neighbourhood



def main():
    global data
    with open('airbnb_datasets/neighbourhoods.geojson') as f:
        data = json.load(f)
    results = sqlCtx.sql("SELECT latitude, longitude, id FROM listings WHERE city = 'Seattle'").collect()
    neighbourhoods = Pool(4).map(inner_loop, results)

    print len(neighbourhoods)
    with open('neigh_s.csv', 'w') as csv_f:
        count = 0
        for neighbourhood in neighbourhoods:
            if(neighbourhood != None):
                csv_f.write("%s, %.10f, %.10f, %s\n"%(neighbourhood[0], neighbourhood[1], neighbourhood[2], results[count][2]))
                count += 1

if __name__ == "__main__":
    main()
