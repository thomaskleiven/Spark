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

def main():
    getNumberOfDistinctCities()
    getAverageBookPricePrCityPrNight()
    #getListingsColumnNames()
    getHostsWithHighestIncome()
    getAverageListingsPrHostFromHostTotalListingsCount()
    getBestGuestInEachCity()
    getBestGuest()
    getAveragePricePrNightPrRoom()
    getAverageNumberOfReviewsPrMonth()
    get()
    bookedPrNight()


sc = SparkContext()
sqlCtx = SQLContext(sc)

def getCalendar():
    calendar_rdd = sc.textFile("airbnb_datasets/calendar_us.csv").map(lambda x: x.split('\t')).filter(lambda line: line[0] != "listing_id").map(lambda s: (int(s[0]), s[1], s[2]))
    calendar_fields=[
            StructField('listing_id', IntegerType(), False),                    #name, type, nullable
            StructField('date', StringType(), False),
            StructField('available', StringType(), False)
    ]

    calendar_schema = StructType(calendar_fields)
    return sqlCtx.createDataFrame(calendar_rdd, calendar_schema)                #Create Data Frame

def getReviews():
    reviews_rdd = sc.textFile("airbnb_datasets/reviews_us.csv").map(lambda x: x.split('\t')).filter(lambda line: line[0] != "listing_id").map(lambda s: (int(s[0]), int(s[1]), s[2], int(s[3]), s[4], s[5]))
    reviews_fields=[
            StructField('listing_id', IntegerType(), False),
            StructField('id', IntegerType(), False),
            StructField('date', StringType(), False),
            StructField('reviewer_id', IntegerType(), False),
            StructField('reviewer_name', StringType(), False),
            StructField('comments', StringType(), False)
    ]

    reviews_schema = StructType(reviews_fields)
    return sqlCtx.createDataFrame(reviews_rdd, reviews_schema)                  #Create Data Frame

def getListings():
    listings_rdd = sc.textFile("airbnb_datasets/listings_us.csv").map(lambda x: x.split('\t'))
    head = listings_rdd.filter(lambda line: line[0] == 'access')
    listings_fields=[]

    for name in range(0, len(listings_rdd.take(1)[0])):                         #Create columns to Data Frame
        temp = head.take(1)[0][name]
        listings_fields.append(StructField(temp, StringType(), False))

    listings_rdd = listings_rdd.filter(lambda line: line[0] != 'access')

    listings_schema = StructType(listings_fields)
    return sqlCtx.createDataFrame(listings_rdd, listings_schema)               #Create Data Frame


calendar = getCalendar()
listings = getListings()
reviews = getReviews()
listings = listings.withColumn("price", regexp_replace("price", "\$", ""))
listings = listings.withColumn("price", regexp_replace("price", ",", ""))
listings = listings.withColumn('price', listings['price'].cast(DoubleType()))
listings = listings.withColumn('host_total_listings_count', listings['host_total_listings_count'].cast(DoubleType()))
sqlCtx.registerDataFrameAsTable(calendar, "calendar")                           #Register Data Frame
sqlCtx.registerDataFrameAsTable(reviews, "reviews")
sqlCtx.registerDataFrameAsTable(listings, "listings")

################################################################################
################################################################################ Begin query
################################################################################

def getListingsColumnNames():                                                   ##print columnds in listings
    listings = getListings()
    for name in range(0, len(listings.take(1)[0])):
        print listings.take(1)[0][name]

def getAverageBookPricePrCityPrNight():
    df = sqlCtx.sql("SELECT city, ROUND(AVG(price),2) AS price FROM listings GROUP BY city")
    df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("results/task3a.csv")

def getAveragePricePrNightPrRoom():
    df = sqlCtx.sql("    SELECT DISTINCT city, room_type, MIN(avgprice) FROM \
                        (SELECT lower(ltrim(rtrim(room_type))) AS room_type, \
                        city AS city, round(AVG(price),2) AS avgprice FROM \
                        listings GROUP BY city,room_type ORDER BY city) GROUP BY city, room_type ORDER BY city, room_type")
    df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("results/task3b.csv")

def getAverageNumberOfReviewsPrMonth():
    df =  sqlCtx.sql("  SELECT city, round(SUM(reviews_per_month),2) as count \
                        FROM listings \
                        GROUP BY city \
                        ORDER BY count DESC")
    df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("results/task3c.csv")

def bookedPrNight():
    df = sqlCtx.sql(" SELECT city, round(SUM(reviews_per_month)*((12.0*3.0)/0.7),2) as count \
                        FROM listing \
                        GROUP BY city \
                        ORDER BY count DESC")
    df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("results/task3d.csv")



def getNumberOfDistinctCities():                                                #Number of distinct cities
    numCitys = sqlCtx.sql("SELECT COUNT(DISTINCT LOWER(LTRIM(RTRIM(city)))) FROM listings").collect()
    cities = sqlCtx.sql("SELECT DISTINCT LOWER(LTRIM(RTRIM(city))) FROM listings").collect()
    with open('task3.csv', 'w') as txt_file:
        txt_file.write("Number of distinct cities, %s \n"%numCitys[0])
        txt_file.write("Cities \n")
        for city in cities:
            txt_file.write("%s \n"%city[0])

def getNumberOfDistinctValuesInListingsColumns():                               #Number of distinct values in each column in listings
    listings = getListings()
    for name in range(0, len(listings.take(1)[0])):
        sqlCtx.sql("SELECT COUNT(DISTINCT %s) AS %s FROM listings"%(listings.take(1)[0][name], listings.take(1)[0][name])).show()


def getAverageListingsPrHostFromHostTotalListingsCount():
    avgListCount = sqlCtx.sql("  SELECT ROUND(AVG(host_total_listings_count),1) AS avg FROM listings").collect()
    numHosts = sqlCtx.sql("  SELECT COUNT(DISTINCT host_id) FROM listings WHERE host_total_listings_count > 1").collect()
    totNumHosts = sqlCtx.sql("  SELECT COUNT(DISTINCT host_id) FROM listings").collect()
    percentage = float(numHosts[0][0])/float(totNumHosts[0][0])
    with open('task4b.csv','w') as csv_f:
        csv_f.write("Percentage, %.2f"%float(percentage))
    with open('task4a.csv', 'w') as csv_f:
        csv_f.write("AverageListingCoutPrHost, %s"%avgListCount[0])

def getBestGuest():
    joined = reviews.join(calendar, reviews.listing_id == calendar.listing_id)
    joined = joined.join(listings, joined.id == listings.id)
    joined.createOrReplaceTempView('joined')
    df = sqlCtx.sql("   SELECT reviewer_id, reviewer_name, SUM(price) as p FROM joined \
                        WHERE available = 'f' GROUP BY reviewer_id, reviewer_name ORDER BY p DESC")

    df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("results/task5b.csv")

def getBestGuestInEachCity():
    #cities = ['New York', 'Seattle', 'San Francisco']
    joined = reviews.join(listings, reviews.listing_id == listings.id)
    joined.createOrReplaceTempView('joined')

    joined = sqlCtx.sql("SELECT city, reviewer_id, reviewer_name, COUNT(listing_id) AS bookings FROM joined GROUP BY city, reviewer_id, reviewer_name ORDER BY bookings DESC")
    joined.createOrReplaceTempView('joined')

    df = sqlCtx.sql("SELECT city, reviewer_id, reviewer_name, bookings FROM (SELECT city, reviewer_id, reviewer_name, bookings, dense_rank() OVER (PARTITION BY city ORDER BY bookings DESC) as rank FROM joined GROUP BY city, reviewer_id, reviewer_name, bookings) tmp WHERE rank <= 3 GROUP BY city, reviewer_id, reviewer_name, bookings")

    df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("results/task5a.csv")

def getHostsWithHighestIncome():
    joined = listings.join(calendar, listings.id == calendar.listing_id)
    joined.createOrReplaceTempView('joined')

    joined = sqlCtx.sql("SELECT city, host_id, host_name, SUM(price) AS totprice FROM joined WHERE available = 'f' GROUP BY city, host_id, host_name ORDER BY totprice DESC")
    joined.createOrReplaceTempView('joined')

    df = sqlCtx.sql("SELECT city, host_id, host_name, totprice FROM (SELECT city, host_id, host_name, totprice, dense_rank() OVER (PARTITION BY city ORDER BY totprice DESC) AS rank FROM joined GROUP BY city, host_name, host_id, totprice) tmp WHERE rank <= 3 GROUP BY city, host_id, host_name, totprice")

    df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("results/task4c.csv")



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



def get():
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
