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

def main():
    #getNumberOfDistinctCities()
    #getAverageBookPricePrCityPrNight()
    #getListingsColumnNames()
    #getAverageListingsPrHost()
    #getPercentageOfHostsWithMoreThanOneListing()
    #getHostsWithHighestIncome()
    getAverageListingsPrHostFromHostTotalListingsCount()
    #getMaxReviewers()
    #getBestGuestInEachCity()
    #getBestGuest()
    #getAveragePricePrNightPrRoom()
    #getAverageNumberOfReviewsPrMonth()
    #getNeighborHood()


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
    pri = sqlCtx.sql("SELECT city, ROUND(AVG(price),2) FROM listings GROUP BY city").collect()
    #print len(pri)
    with open('task3.txt', 'a') as txt_file:
        for i in range(0, len(pri)):
            city = pri[i][0]
            price = pri[i][1]
            txt_file.write("Average price pr night in %s: $%.2f \n"%(city, price))


def getAveragePricePrNightPrRoom():
    result = sqlCtx.sql("       SELECT DISTINCT city, room_type, MIN(avgprice) FROM \
                                (SELECT lower(ltrim(rtrim(room_type))) AS room_type, \
                                city AS city, round(AVG(price),2) AS avgprice FROM \
                                listings GROUP BY city,room_type ORDER BY city) GROUP BY city, room_type ORDER BY city, room_type").collect()

    with open('task3.txt', 'a') as txt_file:
        for i in range(0, len(result)):
            txt_file.write("Average price for %s in %s is $%.2f\n"%(result[i][1], result[i][0], result[i][2]))

def getAverageNumberOfReviewsPrMonth():
    result =  sqlCtx.sql(" SELECT city, round(SUM(reviews_per_month),2) as count \
                        FROM listings \
                        GROUP BY city \
                        ORDER BY count DESC").collect()

    with open('task3.txt', 'a') as txt_file:
        txt_file.write("Average number of reviews pr month in %s is %d\n"%(result[0][0], result[0][1]))
        txt_file.write("Average number of reviews pr month in %s is %d\n"%(result[1][0], result[1][1]))
        txt_file.write("Average number of reviews pr month in %s is %d\n"%(result[2][0], result[2][1]))


def getNumberOfDistinctCities():                                                #Number of distinct cities
    numCitys = sqlCtx.sql("SELECT COUNT(DISTINCT LOWER(LTRIM(RTRIM(city)))) FROM listings").collect()
    cities = sqlCtx.sql("SELECT DISTINCT LOWER(LTRIM(RTRIM(city))) FROM listings").collect()
    with open('task3.txt', 'a') as txt_file:
        txt_file.write("Number of distinct cities: %s \n"%numCitys[0])
        txt_file.write("Cities: \n")
        for city in cities:
            txt_file.write("%s \n"%city[0])

def getNumberOfDistinctValuesInListingsColumns():                               #Number of distinct values in each column in listings
    listings = getListings()
    for name in range(0, len(listings.take(1)[0])):
        sqlCtx.sql("SELECT COUNT(DISTINCT %s) AS %s FROM listings"%(listings.take(1)[0][name], listings.take(1)[0][name])).show()


def getHostsWithHighestIncome():

    cities = ['New York', 'Seattle', 'San Francisco']
    joined = listings.join(calendar, listings.id == calendar.listing_id)
    joined.createOrReplaceTempView('j')
    for city in cities:
        hosts = sqlCtx.sql("    SELECT city, host_id, host_name, SUM(price) AS t FROM j\
                                WHERE available = 'f' AND city = '%s'\
                                GROUP BY host_id, host_name, city \
                                ORDER BY city DESC, t DESC LIMIT 3"%(city)).collect()
        with open('task4.txt', 'a') as txt_file:
            txt_file.write("The host with highest income in %s is named %s, has ID-number: %s and has total income of: $%.2f \n" %(hosts[0][0], hosts[0][2], hosts[0][1], hosts[0][3]))
            txt_file.write("The host with the second highest income in %s is named %s, has ID-number: %s and has total income of: $%.2f \n"%(hosts[1][0], hosts[1][2], hosts[1][1], hosts[1][3]))
            txt_file.write("The host with the third highest income in %s is named %s, has ID-number: %s and has total income of: $%.2f \n"%(hosts[2][0], hosts[2][2], hosts[2][1], hosts[2][3]))


def getAverageListingsPrHostFromHostTotalListingsCount():
    avgListCount = sqlCtx.sql("  SELECT ROUND(AVG(host_total_listings_count),1) AS avg FROM listings").collect()
    numHosts = sqlCtx.sql("  SELECT COUNT(DISTINCT host_id) FROM listings WHERE host_total_listings_count > 1").collect()
    totNumHosts = sqlCtx.sql("  SELECT COUNT(DISTINCT host_id) FROM listings").collect()
    percentage = float(numHosts[0][0])/float(totNumHosts[0][0])
    print numHosts
    print percentage
    with open('task4.txt', 'w') as txt_file:
        txt_file.write("Average number of listings pr host: %s \n"%(avgListCount[0]))
        txt_file.write("Percentage of hosts with more than one listing: %.2f \n"%(percentage))
    ##print sqlCtx.sql( " SELECT MAX(host_total_listings_count) AS totallistings, host_id FROM listings GROUP BY host_id ORDER BY totallistings DESC").show(500)


def getBestGuest():
    joined = reviews.join(calendar, reviews.listing_id == calendar.listing_id)
    joined = joined.join(listings, joined.id == listings.id)
    joined.createOrReplaceTempView('joined')
    result = sqlCtx.sql("    SELECT reviewer_id, reviewer_name, SUM(price) as p FROM joined \
                    WHERE available = 'f' GROUP BY reviewer_id, reviewer_name ORDER BY p DESC").collect()
    with open('task5.txt', 'a') as txt_file:
        txt_file.write("The host who has spent the most money is named %s has ID-number: %s and has spent $%.2f"%(result[0][1], result[0][0], result[0][2]))

def getBestGuestInEachCity():
    #cities = ['New York', 'Seattle', 'San Francisco']
    joined = reviews.join(listings, reviews.listing_id == listings.id)
    joined.createOrReplaceTempView('joined')

    joined = sqlCtx.sql("SELECT city, reviewer_id, reviewer_name, COUNT(listing_id) AS bookings FROM joined GROUP BY city, reviewer_id, reviewer_name ORDER BY bookings DESC")
    joined.createOrReplaceTempView('joined')

    sqlCtx.sql("SELECT city, reviewer_id, reviewer_name, bookings FROM (SELECT city, reviewer_id, reviewer_name, bookings, dense_rank() OVER (PARTITION BY city ORDER BY bookings DESC) as rank FROM joined GROUP BY city, reviewer_id, reviewer_name, bookings) tmp WHERE rank <= 3 GROUP BY city, reviewer_id, reviewer_name, bookings").show()




def getNeighborHood():
    with open('airbnb_datasets/neighbourhoods.geojson') as f:
        data = json.load(f)
    results = sqlCtx.sql("SELECT latitude, longitude, id FROM listings WHERE city = 'New York'").collect()

    assert( len(data['features'][0]['geometry']['coordinates'][0]) == 1 )
    assert( len(data['features'][0]['geometry']['coordinates']) == 1 )
    cross = 0
    num = 0
    neighbourhoods = np.array([None]*len(results))

    for result in results:
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
                neighbourhoods[i] = feature['properties']['neighbourhood'], xPos, yPos
                break

    with open('neighs.csv', 'wb') as txt_file:
        writer = csv.writer(txt_file)
        writer.writerows(neighbourhoods)


if __name__ == "__main__":
    main()

"""


def getNeighborHood():
    global data
    with open('airbnb_datasets/neighbourhoods.geojson') as f:
        data = json.load(f)
    results = sqlCtx.sql("SELECT latitude, longitude, id FROM listings WHERE city = 'New York'").collect()

    assert( len(data['features'][0]['geometry']['coordinates'][0]) == 1 )
    assert( len(data['features'][0]['geometry']['coordinates']) == 1 )

    pool = Pool(processes=4)
    neighbourhoods = pool.map(inner_loop, results)


def inner_loop(result):
    xPos = float(result[0])
    yPos = float(result[1])
    cross = 0
    for feature in data['features']:
        N = len(feature['geometry']['coordinates'][0][0])
        j = N - 1
        vertx = np.zeros(N)
        verty = np.zeros(N)

        for k in range(0, N):
            vertx[k] = float(feature['geometry']['coordinates'][0][0][k][1])
            verty[k] = float(feature['geometry']['coordinates'][0][0][k][0])
        for i in range(0,N - 1):
            if(((verty[i] > yPos) != (verty[j]>yPos)) and (xPos < (vertx[j] - vertx[i]) * ((yPos-verty[i])/(verty[j]-verty[i]))+vertx[i])):
                cross += 1
            j = i

        if(cross%2 != 0):
            print feature['properties']['neighbourhood']
            return feature['properties']['neighbourhood']
        else:
            return "NN"

"""
