from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import unix_timestamp, dayofmonth, year, date_format, udf, col, ltrim, rtrim, trim
from pyspark.sql import SQLContext, DataFrame
from pyspark.sql.types import *
import os
import tempfile

conf = SparkConf().setAppName("AirBnB listing")
sc = SparkContext(conf = conf)
sqlCtx = SQLContext(sc)

#Take in a RDD and return a dataFrame
def makeDataFrame(RDD):
    listings_fields = []
    for name in range(0, len(RDD.take(1)[0])):                         #Create columns to Data Frame
        temp = RDD.take(1)[0][name]
        #print temp
        listings_fields.append(StructField(temp, StringType(), False))
    header = RDD.take(1)[0]
    rowsIWant = RDD.filter(lambda line: line != header)
    listings_schema = StructType(listings_fields)
    #print "\n\n\n", listings_schema
    return sqlCtx.createDataFrame(rowsIWant, listings_schema)                #Create Data Frame

#Data from its'learning, listing and test-file
neighborhoodFile = sc.textFile("data/Seattle_neighborhood.csv") #get neighborhood test
listingFile = sc.textFile("data/Seattle_neighborhood.csv") #get filter

neighborhoodFile = neighborhoodFile.map(lambda x: x.split('\t'))
listingFile = listingFile.map(lambda x: x.split('\t'))

neighborDf = makeDataFrame(neighborhoodFile)
listingDf = makeDataFrame(listingFile)

# Seattle
seattle_ray_casting = sc.textFile("data/Seattle_neighborhood.csv") #seattle data with listings_id connected to neighborhoods
seattle_ray_casting2 = sc.textFile("data/Seattle_neighborhood.csv") #seattle data with neighbourhood_groups instead of neighbourhood

seattleRayFile = seattle_ray_casting.map(lambda x: x.split(','))
seattleRayFile2 = seattle_ray_casting2.map(lambda x: x.split(','))

#Making the dataframe for Seattle-csv without the column names
seattle_field = []
seattle_field.append(StructField("neighbourhood", StringType(), False))
seattle_field.append(StructField("lat", StringType(), False))
seattle_field.append(StructField("lon", StringType(), False))
seattle_field.append(StructField("id", StringType(), False))
seattle_schema = StructType(seattle_field)
seattleDf = sqlCtx.createDataFrame(seattleRayFile, seattle_schema)
seattleDf = seattleDf.withColumn('id', ltrim(seattleDf.id))

seattle2Df = makeDataFrame(seattleRayFile2)
#seattle2Df.show()

#New York
new_york_ray_casting = sc.textFile("data/New_York_neighborhood.csv")
newYorkRayFile = new_york_ray_casting.map(lambda x: x.split(','))

newYorkDf = makeDataFrame(newYorkRayFile)
newYorkDf = newYorkDf.withColumn('id', ltrim(newYorkDf.id))

#newYorkDf.show()

#San Francisco
san_francisco_ray_casting = sc.textFile("data/San_Francisco_neighborhood.csv")
sanFranciscoRayFile = san_francisco_ray_casting.map(lambda x: x.split(','))

seattle_field = []
seattle_field.append(StructField("neighbourhood", StringType(), False))
seattle_field.append(StructField("lat", StringType(), False))
seattle_field.append(StructField("lon", StringType(), False))
seattle_field.append(StructField("id", StringType(), False))
seattle_schema = StructType(seattle_field)
sanFranciscoDf = sqlCtx.createDataFrame(sanFranciscoRayFile, seattle_schema)
sanFranciscoDf = sanFranciscoDf.withColumn('id', ltrim(sanFranciscoDf.id))

#joins test-file and listings to get coordinates for the test-file, not used in final delivery, but here to show the method
def joinListingReview():
    df1 = neighborDf.alias("neighborDf")
    df2 = listingDf.alias("listingDf")

    df2 = df2.select("id", "longitude", "latitude", "price", "city").filter(df2.city == 'Seattle')

    joined_df = df1.join(df2,col("listingDf.id") == col("neighborDf.id"), 'inner')
    joined_df = joined_df.select("listingDf.id", "listingDf.latitude", "listingDf.longitude", "neighborDf.neighbourhood", "neighborDf.city")
    return joined_df

#Joining Seattle-data with listing attributes
def seattleWithInterestingData():
    listingTDf = listingDf.select(listingDf.id, listingDf.price, listingDf.city, listingDf.xl_picture_url).filter(listingDf.city == 'Seattle')
    joined_df = seattle2Df.join(listingTDf,["id"])
    joined_df = joined_df.sort("neighbourhood")
    joined_df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("results/SeattleVisualization.csv")

#Joining New York-data with listing attribute
def newYorkWithInterestingData():
    listingTDf = listingDf.select(listingDf.id, listingDf.price, listingDf.city, listingDf.review_scores_value,listingDf.review_scores_rating,listingDf.extra_people).filter(listingDf.city == 'New York')
    joined_df = newYorkDf.join(listingTDf,["id"])

    joined_df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("results/NewYorkVisualization.csv")

#Joining San Francisco-data with listing attribute
def sanFranciscoWithInterestingData():
    listingTDf = listingDf.select(listingDf.id, listingDf.price, listingDf.city, listingDf.review_scores_value,listingDf.review_scores_rating,listingDf.extra_people,listingDf.xl_picture_url).filter(listingDf.city == 'San Francisco')
    joined_df = sanFranciscoDf.join(listingTDf,["id"])
    joined_df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("results/SanFranciscoVisualization.csv")


#Comparing Seattle-data from the algorithm with results from the test file
def resultAccuracy():
    listingTDf = listingDf.select("id", "longitude", "latitude")
    test = neighborDf.select("id", "neighbourhood").sort("id")
    result = seattleDf.select("id", "neighbourhood", "lon", "lat").sort("id")

    test = test.join(listingTDf, ["id"])
    #test = test.join(result, ["id"]).select("id", test.neighbourhood)
    sqlCtx.registerDataFrameAsTable(test, "test")
    sqlCtx.registerDataFrameAsTable(result, "result")

    compare = sqlCtx.sql(    "SELECT 100.0*(SELECT 1.0*COUNT(*) AS count \
                                    FROM \
                                    (SELECT * \
                                    FROM test \
                                    INNER JOIN result \
                                    ON test.id = result.id \
                                    AND test.neighbourhood = result.neighbourhood)) / (1.0*COUNT(*)) AS Hit FROM test")
    compare.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("results/CompareResultWithTest.csv")

def main():
    seattleWithInterestingData()
    newYorkWithInterestingData()
    sanFranciscoWithInterestingData()
    resultAccuracy()


if __name__ == "__main__":
    main()

######################## SQL QUERIES USED ON CARTO #########################

#use in Carto to get the geometric-column correctly
#ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)

#query to get all the geoms in the right neighbourhood
#SELECT new_york_neighbourhood_group.the_geom, new_york_neighbourhood_group.neighborhood,COUNT(*) AS count FROM new_york_neighbourhood_group JOIN neighbourhoods ON ST_Intersects(new_york_neighbourhood_group.the_geom, neighbourhoods.the_geom) WHERE new_york_neighbourhood_group.neighborhood LIKE neighbourhoods.neighbourhood GROUP BY new_york_neighbourhood_group.the_geom, new_york_neighbourhood_group.neighborhood
#SELECT COUNT(*) / (SELECT COUNT(*) FROM (SELECT seattle1_0.the_geom, seattle1_0.neighbourhood AS count FROM seattle1_0 JOIN neighbourhoods ON ST_Intersects(seattle1_0.the_geom, neighbourhoods.the_geom) WHERE seattle1_0.neighbourhood LIKE neighbourhoods.neighbourhood GROUP BY seattle1_0.the_geom, seattle1_0.neighbourhood) AS a) FROM seattle1_0
#SELECT 100.0 * 1.0 / (1.0*COUNT(*) / (SELECT 1.0*COUNT(*) FROM (SELECT seattle1_0.the_geom, seattle1_0.neighbourhood AS count FROM seattle1_0 JOIN neighbourhoods ON ST_Intersects(seattle1_0.the_geom, neighbourhoods.the_geom) WHERE seattle1_0.neighbourhood LIKE neighbourhoods.neighbourhood GROUP BY seattle1_0.the_geom, seattle1_0.neighbourhood) AS a)) FROM seattle1_0
#SELECT 100*(SELECT 1.0*COUNT(*) FROM (SELECT new_york_neighbourhood_group.the_geom, new_york_neighbourhood_group.neighborhood,COUNT(*) AS count FROM new_york_neighbourhood_group JOIN neighbourhoods ON ST_Intersects(new_york_neighbourhood_group.the_geom, neighbourhoods.the_geom) WHERE new_york_neighbourhood_group.neighborhood LIKE neighbourhoods.neighbourhood GROUP BY new_york_neighbourhood_group.the_geom, new_york_neighbourhood_group.neighborhood) as a) / (1.0*COUNT(*)) FROM new_york_neighbourhood_group

#            QUERY FOR SEATTLE
#sqlCtx.sql( "SELECT 100.0 *( \
#            SELECT 1.0*COUNT(*) FROM \
#            (SELECT seattle1_0.the_geom, seattle1_0.neighbourhood AS count \
#            FROM seattle1_0 \
#            JOIN neighbourhoods \
#            ON ST_Intersects(seattle1_0.the_geom, neighbourhoods.the_geom) \
#            WHERE seattle1_0.neighbourhood LIKE neighbourhoods.neighbourhood \
#            GROUP BY seattle1_0.the_geom, seattle1_0.neighbourhood) AS a) / (1.0*COUNT(*)) \
#            FROM seattle1_0")

#print seattleDf.show(100)

#SELECT 100.0 *(SELECT 1.0*COUNT(*) FROM (SELECT san_francisco_neighbourhood.the_geom, san_francisco_neighbourhood.neighbourhood AS count FROM san_francisco_neighbourhood JOIN neighbourhoods ON ST_Intersects(san_francisco_neighbourhood.the_geom, neighbourhoods.the_geom) WHERE san_francisco_neighbourhood.neighbourhood LIKE neighbourhoods.neighbourhood GROUP BY san_francisco_neighbourhood.the_geom, san_francisco_neighbourhood.neighbourhood) AS a) / (1.0*COUNT(*)) FROM san_francisco_neighbourhood
