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
import numpy as np
from multiprocessing import Pool


def main():
    air = airbnb()

    air.parallellize_neighbor()
    #air.getNumberOfDistinctCities()
    #air.getAverageBookPricePrCityPrNight()
    #getListingsColumnNames()
    #air.getHostsWithHighestIncome()
    #air.getAverageListingsPrHostFromHostTotalListingsCount()
    #air.getBestGuestInEachCity()
    #air.getBestGuest()
    #air.getAveragePricePrNightPrRoom()
    #air.getAverageNumberOfReviewsPrMonth()
    #air.bookedPrNight()

class airbnb():
    def __init__(self):
        self.sc = SparkContext()
        self.sqlCtx = SQLContext(self.sc)

        self.calendar = self.getCalendar()
        self.listings = self.getListings()
        self.reviews = self.getReviews()
        self.listings = self.listings.withColumn("price", regexp_replace("price", "\$", ""))
        self.listings = self.listings.withColumn("price", regexp_replace("price", ",", ""))
        self.listings = self.listings.withColumn('host_total_listings_count', self.listings['host_total_listings_count'].cast(DoubleType()))
        self.listings = self.listings.withColumn('price', self.listings['price'].cast(DoubleType()))
        self.sqlCtx.registerDataFrameAsTable(self.calendar, "calendar")         #Register Data Frame
        self.sqlCtx.registerDataFrameAsTable(self.reviews, "reviews")
        self.sqlCtx.registerDataFrameAsTable(self.listings, "listings")

    def getCalendar(self):
        calendar_rdd = self.sc.textFile("airbnb_datasets/calendar_us.csv").map(lambda x: x.split('\t')).filter(lambda line: line[0] != "listing_id").map(lambda s: (int(s[0]), s[1], s[2]))
        calendar_fields=[
                StructField('listing_id', IntegerType(), False),                #name, type, nullable
                StructField('date', StringType(), False),
                StructField('available', StringType(), False)
        ]

        calendar_schema = StructType(calendar_fields)
        return self.sqlCtx.createDataFrame(calendar_rdd, calendar_schema)       #Create Data Frame

    def getReviews(self):
        reviews_rdd = self.sc.textFile("airbnb_datasets/reviews_us.csv").map(lambda x: x.split('\t')).filter(lambda line: line[0] != "listing_id").map(lambda s: (int(s[0]), int(s[1]), s[2], int(s[3]), s[4], s[5]))
        reviews_fields=[
                StructField('listing_id', IntegerType(), False),                #name, type, nullable
                StructField('id', IntegerType(), False),
                StructField('date', StringType(), False),
                StructField('reviewer_id', IntegerType(), False),
                StructField('reviewer_name', StringType(), False),
                StructField('comments', StringType(), False)
        ]

        reviews_schema = StructType(reviews_fields)
        return self.sqlCtx.createDataFrame(reviews_rdd, reviews_schema)         #Create Data Frame

    def getListings(self):
        listings_rdd = self.sc.textFile("airbnb_datasets/listings_us.csv").map(lambda x: x.split('\t'))
        head = listings_rdd.filter(lambda line: line[0] == 'access')
        listings_fields=[]

        for name in range(0, len(listings_rdd.take(1)[0])):                     #Create columns to Data Frame
            temp = head.take(1)[0][name]
            listings_fields.append(StructField(temp, StringType(), False))      #@Params: name, type, nullable

        listings_rdd = listings_rdd.filter(lambda line: line[0] != 'access')

        listings_schema = StructType(listings_fields)
        return self.sqlCtx.createDataFrame(listings_rdd, listings_schema)       #Create Data Frame

################################################################################
################################################################################ Begin query
################################################################################

    def getListingsColumnNames(self):
        listings = getListings()
        for name in range(0, len(listings.take(1)[0])):
            print listings.take(1)[0][name]

    def getAverageBookPricePrCityPrNight(self):
        df = self.sqlCtx.sql("SELECT city, ROUND(AVG(price),2) AS price FROM listings GROUP BY city")
        df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("results/task3a.csv")

    def getAveragePricePrNightPrRoom(self):
        df = self.sqlCtx.sql("  SELECT DISTINCT city, room_type, MIN(avgprice) FROM \
                                (SELECT lower(ltrim(rtrim(room_type))) AS room_type, \
                                city AS city, round(AVG(price),2) AS avgprice FROM \
                                listings GROUP BY city,room_type ORDER BY city) GROUP BY city, room_type ORDER BY city, room_type")
        df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("results/task3b.csv")

    def getAverageNumberOfReviewsPrMonth(self):
        df =  self.sqlCtx.sql(" SELECT city, round(SUM(reviews_per_month),2) as count \
                                FROM listings \
                                GROUP BY city \
                                ORDER BY count DESC")
        df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("results/task3c.csv")

    def bookedPrNight(self):
        df = self.sqlCtx.sql("  SELECT city, round(SUM(reviews_per_month)*((12.0*3.0)/0.7),2) as count \
                                FROM listing \
                                GROUP BY city \
                                ORDER BY count DESC")
        df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("results/task3d.csv")



    def getNumberOfDistinctCities(self):                                        #Number of distinct cities
        numCitys = self.sqlCtx.sql("SELECT COUNT(DISTINCT LOWER(LTRIM(RTRIM(city)))) FROM listings").collect()
        cities = self.sqlCtx.sql("SELECT DISTINCT LOWER(LTRIM(RTRIM(city))) FROM listings").collect()
        with open('task3.csv', 'w') as txt_file:
            txt_file.write("Number of distinct cities, %s \n"%numCitys[0])
            txt_file.write("Cities \n")
            for city in cities:
                txt_file.write("%s \n"%city[0])

    def getNumberOfDistinctValuesInListingsColumns(self):                       #Number of distinct values in each column in listings
        listings = getListings()
        for name in range(0, len(listings.take(1)[0])):
            self.sqlCtx.sql("SELECT COUNT(DISTINCT %s) AS %s FROM listings"%(listings.take(1)[0][name], listings.take(1)[0][name])).show()


    def getAverageListingsPrHostFromHostTotalListingsCount(self):
        avgListCount = self.sqlCtx.sql("  SELECT ROUND(AVG(host_total_listings_count),1) AS avg FROM listings").collect()
        numHosts = self.sqlCtx.sql("  SELECT COUNT(DISTINCT host_id) FROM listings WHERE host_total_listings_count > 1").collect()
        totNumHosts = self.sqlCtx.sql("  SELECT COUNT(DISTINCT host_id) FROM listings").collect()
        percentage = float(numHosts[0][0])/float(totNumHosts[0][0])
        with open('task4b.csv','w') as csv_f:
            csv_f.write("Percentage, %.2f"%float(percentage))
        with open('task4a.csv', 'w') as csv_f:
            csv_f.write("AverageListingCoutPrHost, %s"%avgListCount[0])

    def getBestGuest(self):
        joined = self.reviews.join(self.calendar, self.reviews.listing_id == self.calendar.listing_id)
        joined = joined.join(self.listings, joined.id == self.listings.id)
        joined.createOrReplaceTempView('joined')
        df = self.sqlCtx.sql("   SELECT reviewer_id, reviewer_name, SUM(price) as p FROM joined \
                                WHERE available = 'f' GROUP BY reviewer_id, reviewer_name ORDER BY p DESC")

        df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("results/task5b.csv")

    def getBestGuestInEachCity(self):
        joined = self.reviews.join(self.listings, self.reviews.listing_id == self.listings.id)
        joined.createOrReplaceTempView('joined')

        joined = self.sqlCtx.sql("SELECT city, reviewer_id, reviewer_name, COUNT(listing_id) AS bookings FROM joined GROUP BY city, reviewer_id, reviewer_name ORDER BY bookings DESC")
        joined.createOrReplaceTempView('joined')

        df = self.sqlCtx.sql("SELECT city, reviewer_id, reviewer_name, bookings FROM (SELECT city, reviewer_id, reviewer_name, bookings, dense_rank() OVER (PARTITION BY city ORDER BY bookings DESC) as rank FROM joined GROUP BY city, reviewer_id, reviewer_name, bookings) tmp WHERE rank <= 3 GROUP BY city, reviewer_id, reviewer_name, bookings")
        df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("results/task5a.csv")

    def getHostsWithHighestIncome(self):
        joined = self.listings.join(self.calendar, self.listings.id == self.calendar.listing_id)
        joined.createOrReplaceTempView('joined')

        joined = self.sqlCtx.sql("SELECT city, host_id, host_name, SUM(price) AS totprice FROM joined WHERE available = 'f' GROUP BY city, host_id, host_name ORDER BY totprice DESC")
        joined.createOrReplaceTempView('joined')

        df = self.sqlCtx.sql("SELECT city, host_id, host_name, totprice FROM (SELECT city, host_id, host_name, totprice, dense_rank() OVER (PARTITION BY city ORDER BY totprice DESC) AS rank FROM joined GROUP BY city, host_name, host_id, totprice) tmp WHERE rank <= 3 GROUP BY city, host_id, host_name, totprice")
        df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("results/task4c.csv")

    def parallellize_neighbor(self):
        results = self.sqlCtx.sql("SELECT latitude, longitude, id FROM listings WHERE city = 'Seattle'").collect()
        global data
        with open('airbnb_datasets/neighbourhoods.geojson') as f:
            data = json.load(f)
        neighbourhoods = Pool(4).map(inner_loop, results)                       #Parallellize into four independent subtasks
        with open('neigh_s.csv', 'w') as csv_f:
            count = 0
            for neighbourhood in neighbourhoods:
                if(neighbourhood != None):
                    csv_f.write("%s, %.10f, %.10f, %s\n"%(neighbourhood[0], neighbourhood[1], neighbourhood[2], results[count][2]))
                    count += 1

def inner_loop(result):                                                         #Inner loop in the parallell

    xPos = float(result[0])
    yPos = float(result[1])
    for feature in data['features']:
        N = len(feature['geometry']['coordinates'][0][0])                       #Number of corners in polygon
        j = N - 1
        vertx = np.zeros(N)                                                     #X-crd of polygon
        verty = np.zeros(N)                                                     #Y-crd of polygon

        xmax = 0
        xmin = 1E6
        ymax = 0
        ymin = 1E6
        for k in range(0, N):
            vertx[k] = float(feature['geometry']['coordinates'][0][0][k][1])    #Collect x-crd of vertices in given polygon
            verty[k] = float(feature['geometry']['coordinates'][0][0][k][0])    #Collect y-crd of vertices in given polygon
            if(vertx[k] > xmax): xmax = vertx[k]                                #Collect min/max values of polygon
            if(vertx[k] < xmin): xmin = vertx[k]
            if(verty[k] > ymax): ymax = verty[k]
            if(verty[k] < ymin): ymin = verty[k]

        cross = 0
        if(xPos < xmin or xPos > xmax or yPos < ymin or yPos > ymax):           #Check if given point is inside max/min values of given polygon
            continue
        #Compare each slope from given point to each corner, and check if given point is inside or outside y-domain of polygon
        for i in range(0,N - 1):
            if(((verty[i] > yPos) != (verty[j]>yPos)) and (xPos < (vertx[j] - vertx[i]) * ((yPos-verty[i])/(verty[j]-verty[i]))+vertx[i])):
                cross += 1
            j = i
        if(cross%2 != 0):
            neighbourhood = [feature['properties']['neighbourhood'], xPos, yPos]
            return neighbourhood


if __name__ == "__main__":
    main()
