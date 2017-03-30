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
        self.sc = SparkContext()
        self.sqlCtx = SQLContext(self.sc)
        self.spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

        self.listings = self.getListings()      #dataframe for listings
        self.calender = self.getCalender()      #dataframe for calender
        self.header = ["id", "city", "price", "room_type","longitude", "latitude", "amenities", "name", "picture_url", "review_scores_rating", "last_review"]
        self.listingsStripped = self.getWantedColumns(self.listings, self.header)   #dataframe for listings with only relevant columns

        self.userInput, self.listingChosen = self.inputFromUser()       #return two arrays with user input and the chosen listing
        self.sortByCityPriceDistance()                                  #sorts listingsStripped after City, Price and Distance
        self.listingsCalender = self.joinListingsWithCalender()         #join listingsStripped with Calender
        self.sortByDateAvailability()                                   #sorts listingsCalender after Date and Availability

        self.findCommonAmenities()                                      #adds number of common amenities
        self.findTopAlternatives()                                      #write the top n alternatives to file

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
            writer = csv.DictWriter(f, fieldnames=self.header)
            writer.writeheader()
            writer.writerow(rowDict)
        return value

    #Returns row as array
    def getRowAsArray(self,df):
        rowList = df.rdd.flatMap(list).collect()
        return rowList

    #Calculates the maximum amount a user is willing to pay for a accomdation
    def calculateMaxPrice(self,price,maxIncrease):
        return (price*(100.0+maxIncrease)/100.0)

    #Haversin formula to calculate distance
    def maxDistance(self,phi1,lambda1,phi2,lambda2):
        r = 6378
        print phi2.first()
        phi1,phi2,lambda1,lambda2 = map(np.radians, [phi1,phi2,lambda1,lambda2])
        d = 2*r*np.arcsin(np.sqrt(np.square(np.sin((phi2-phi1)/2))+np.cos(phi1)*np.cos(phi2)*np.square(np.sin((lambda2-lambda1)/2))))
        return d

    #Haversin formula adapted to dataframe
    def maxDistanceWithColumn(self, lat, lon):
        r = 6378
        phi1,lambda1 = map(np.radians, [lat,lon])
        self.listingsStripped = (self.listingsStripped
            .withColumn('longitudeRad', self.listingsStripped['longitude']*np.pi/180.0)
            .withColumn('latitudeRad', self.listingsStripped['latitude']*np.pi/180.0))

        self.listingsStripped = (self.listingsStripped
            .withColumn('distance', 2*r*asin(sqrt(pow(sin((self.listingsStripped['latitudeRad']-phi1)/2),2)\
            +np.cos(phi1)*cos(self.listingsStripped['latitudeRad'])*pow(sin((self.listingsStripped['longitudeRad']-lambda1)/2),2)))))

    #Sort result by City, Price, Distance and room_type
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

    #Sort result by Date and Availability on that date
    def sortByDateAvailability(self):
        date = self.userInput[1]
        self.listingsCalender = self.listingsCalender.filter(self.listingsCalender.date == date)\
        .filter(self.listingsCalender.available == "t")

    #Find common Amenities between point from user input given id and the potential alternatives
    def findCommonAmenities(self):
        amenitiesChosen = self.listingChosen[6]
        rows_am = self.listingsCalender.select('id','amenities').collect()

        #Cleans up the amentiesrows to give more accurate results
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
            flat_array.append({'id': str(row.id), 'number_of_common_amenities': int(common_amenities)})

        idCommonAmenities_df = self.spark.createDataFrame(flat_array)

        self.listingsCalender = self.listingsCalender.join(idCommonAmenities_df, ['id'])

    #Output the n best alternatives based on the number of common amenities with listing of the user input given id
    def findTopAlternatives(self):
        headerList = ["id","name","number_of_common_amenities", "distance", "price"]
        self.header.append("number_of_common_amenities")
        self.listingsCalender = self.listingsCalender\
        .select(headerList)\
        .sort('number_of_common_amenities', ascending = False)
        with open ('alternativeListTopN.tsv', 'w') as f:
            writer = csv.DictWriter(f,delimiter ='\t', fieldnames=headerList)
            writer.writeheader()
            for row in self.listingsCalender.limit(int(self.userInput[4])).collect():
                row = row.asDict()
                writer.writerow(row)

    #Writes all potential alternatives to file with interesting columns
    def visualtisationWriter(self):
        self.header.append("number_of_common_amenities")
        self.listingsCalender = self.listingsCalender\
        .select(self.header)\
        .sort('common_amenities', ascending = False)
        with open ('alternativeListTopN.csv', 'w') as f:
            writer = csv.DictWriter(f, fieldnames=self.header)
            writer.writeheader()
            for row in self.listingsCalender.collect():
                row = row.asDict()
                writer.writerow(row)

    #Takes input from user, check enough fields are given and select a listing based on the id
    def inputFromUser(self):
        inputUser = sys.argv[1:]

        if (len(inputUser) != 5):
            raise ValueError("Not enough arguments")

        listingById = self.getListingValueById(inputUser[0])
        listingByIdArray = self.getRowAsArray(listingById)
        return inputUser,listingByIdArray


if __name__ == "__main__":
    main()
