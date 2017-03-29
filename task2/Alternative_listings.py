from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
import sys
from py4j.java_gateway import java_import
import csv
#jvm = sc._gateway.jvm
#java_import(jvm, "com.bla.blu.MySum")


def main():
    alt = Alternative_listing()
    #alt.maxDistanceWithColumn(40.71049137904396,-73.94515406709094)
    #alt.calender.filter(alt.calender.listing_id == "8017041").filter(alt.calender.date == "2016-12-15").show()

class Alternative_listing:
    def __init__(self):
        self.lol = "lol"
        self.sc = SparkContext()
        self.sqlCtx = SQLContext(self.sc)
        self.spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

        self.listings = self.getListings()
        self.calender = self.getCalender()
        self.listingsStrippedHeader = ["id", "city", "price", "room_type","longitude", "latitude", "amenities"]
        self.listingsStripped = self.getWantedColumns(self.listings, self.listingsStrippedHeader)

        #df = self.getDescriptionOnNeighborhood()

        self.userInput, self.listingChosen = self.inputFromUser()
        self.sortByCityPriceDistance()
        self.listingsCalender = self.joinListingsWithCalender()
        self.sortByDateAvailability()

        self.findCommonAmenities()
        self.findTopAlternatives()

    #Gets the listings-data an make it into a dataframe
    def getListings(self):
        df = self.spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t") \
        .load("airbnb_datasets/listings_us.csv")
        return df

    #Gets the calender-data an make it into a dataframe
    def getCalender(self):
        df = self.spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t") \
        .load("airbnb_datasets/calendar_us.csv")
        return df

    #Make a dataframe from df with the columns columns
    def getWantedColumns(self, df, columns):
        df = df.select(columns)
        df = (df
            .withColumn("price",regexp_replace("price","\$",""))
            .withColumn("price",regexp_replace("price",",","")))
        return df

    #Join listings-df with calender-df
    def joinListingsWithCalender(self):
        df = self.listingsStripped.join(self.calender, self.listingsStripped.id == self.calender.listing_id)
        return df

    #Returns the row from listings for the given id
    def getListingValueById(self, id):
        value = self.listingsStripped.where(self.listingsStripped.id == id)
        with open ('listingSelected.csv', 'w') as f:
            row = value.collect()
            rowDict = row[0].asDict()
            writer = csv.DictWriter(f, fieldnames=self.listingsStrippedHeader)
            writer.writeheader()
            writer.writerow(rowDict)
        return value

    #TO DO
    def getRowAsArray(self,df):
        rowList = df.rdd.flatMap(list).collect()
        return rowList

    #Calculates the maximum amount a user is willing to pay for a accomdation
    def calculateMaxPrice(self,price,maxIncrease):
        return (price*(100.0+maxIncrease)/100.0)

    def maxDistance(self,phi1,lambda1,phi2,lambda2):
        r = 6378
        print phi2.first()
        phi1,phi2,lambda1,lambda2 = map(np.radians, [phi1,phi2,lambda1,lambda2])
        d = 2*r*np.arcsin(np.sqrt(np.square(np.sin((phi2-phi1)/2))+np.cos(phi1)*np.cos(phi2)*np.square(np.sin((lambda2-lambda1)/2))))
        return d

    def maxDistanceWithColumn(self, lat, lon):
        r = 6378
        phi1,lambda1 = map(np.radians, [lat,lon])
        self.listingsStripped = (self.listingsStripped
            .withColumn('longitudeRad', self.listingsStripped['longitude']*np.pi/180.0)
            .withColumn('latitudeRad', self.listingsStripped['latitude']*np.pi/180.0))

        self.listingsStripped = (self.listingsStripped
            .withColumn('distance', 2*r*asin(sqrt(pow(sin((self.listingsStripped['latitudeRad']-phi1)/2),2)\
            +np.cos(phi1)*cos(self.listingsStripped['latitudeRad'])*pow(sin((self.listingsStripped['longitudeRad']-lambda1)/2),2)))))

    def sortByCityPriceDistance(self):
        city = self.listingChosen[1]
        maxPrice = self.calculateMaxPrice(float(self.listingChosen[2]), float(self.userInput[2]))
        room_type = self.listingChosen[3]
        longitude,latitude = [float(i) for i in self.listingChosen[4:6]]
        listing_id = self.listingChosen[0]

        self.listingsStripped = self.listingsStripped.filter(self.listingsStripped.city == city)\
        .filter(self.listingsStripped.price <= maxPrice)\
        .filter(self.listingsStripped.room_type == room_type)

        self.maxDistanceWithColumn(latitude,longitude)
        maxDistance = float(self.userInput[3])
        self.listingsStripped = self.listingsStripped.filter(self.listingsStripped.distance <= maxDistance)\
        .filter(self.listingsStripped.id != listing_id)

    def sortByDateAvailability(self):
        date = self.userInput[1]
        self.listingsCalender = self.listingsCalender.filter(self.listingsCalender.date == date)\
        .filter(self.listingsCalender.available == "t")

    def findCommonAmenities(self):
        amenitiesChosen = self.listingChosen[6]
        rows_am = self.listingsCalender.select('id','amenities').collect()

        def parseAmenities(amenities):
            array = amenities\
                .replace('"','')\
                .replace('{','')\
                .replace('}','')\
                .split(',')
            return array

        amenitiesChosenArray = parseAmenities(amenitiesChosen)
        flat_array = []
        for row in rows_am:
            row_amenities = parseAmenities(row.amenities)
            common_amenities = len(np.intersect1d(amenitiesChosenArray,row_amenities))
            flat_array.append({'id': str(row.id), 'common_amenities': int(common_amenities)})

        idCommonAmenities_df = self.spark.createDataFrame(flat_array)

        self.listingsCalender = self.listingsCalender.join(idCommonAmenities_df, ['id'])


    def findTopAlternatives(self):
        headerList = ["id","city", "room_type", "latitude", "longitude", "price", "amenities", "common_amenities"]
        self.listingsCalender = self.listingsCalender\
        .select(headerList)\
        .sort('common_amenities', ascending = False)
        with open ('altListTopN.csv', 'w') as f:
            writer = csv.DictWriter(f, fieldnames=headerList)
            writer.writeheader()
            for row in self.listingsCalender.limit(int(self.userInput[4])).collect():
                row = row.asDict()
                writer.writerow(row)


    def inputFromUser(self):
        inputUser = sys.argv[1:]

        if (len(inputUser) != 5):
            raise ValueError("Not enough arguments")

        listingById = self.getListingValueById(inputUser[0])
        listingByIdArray = self.getRowAsArray(listingById)
        return inputUser,listingByIdArray


    def udf_testing(self, val1, val2):
        return val1+val2



if __name__ == "__main__":
    main()
