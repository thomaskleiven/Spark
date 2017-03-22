from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession, Row
import numpy as np

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
        self.listingsStripped = self.getWantedColumns(self.listings, ["id", "price", "room_type","longitude", "latitude"])
        self.listingsCalender = self.joinListingsWithCalender()
        #df = self.getDescriptionOnNeighborhood()

    #Gets the listings-data an make it into a dataframe
    def getListings(self):
        df = self.spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t") \
        .load("airbnb_data/listings2_us.csv")
        return df

    #Gets the calender-data an make it into a dataframe
    def getCalender(self):
        df = self.spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t") \
        .load("airbnb_data/calendar2_us.csv")
        return df

    #Make a dataframe from df with the columns columns
    def getWantedColumns(self, df, columns):
        df = df.select(columns)
        return df

    #Join listings-df with calender-df
    def joinListingsWithCalender(self):
        df = self.listingsStripped.join(self.calender, self.listingsStripped.id == self.calender.listing_id)
        return df

    #Returns the row from listings for the given id
    def getListingValueById(self, id, date):
        value = self.listingsCalender.where((self.listingsCalender.id == id) & (self.listingsCalender.date == date))
        return value

    #TO DO
    def getRowAsArray(self,df):
        rowList = df.rdd.flatMap(list).collect()
        return rowList

    #Calculates the maximum amount a user is willing to pay for a accomdation
    def calculateMaxPrice(self,price,maxIncrease):
        return (price*(1+maxIncrease))

    def maxDistance(self,phi1,lambda1,phi2,lambda2):
        r = 6378
        phi1,phi2,lambda1,lambda2 = map(np.radians, [phi1,phi2,lambda1,lambda2])
        d = 2*r*np.arcsin(np.sqrt(np.square(np.sin((phi2-phi1)/2))+np.cos(phi1)*np.cos(phi2)*np.square(np.sin((lambda2-lambda1)/2))))
        return d

def main():
    alt = Alternative_listing()
    #alt.getListingValueById("10034090", "2017-09-24").show()
    #print alt.getListingValueById("10034090", "2017-09-24").rdd.flatMap(list).collect()[1]
    #hi = alt.getListingValueById("10034090", "2017-09-24")
    #hi.show()
    #print alt.getRowAsArray(hi)
    print alt.maxDistance(40.7467268434,-73.9407464435,40.7396389651,-73.9586614586)

if __name__ == "__main__":
    main()
