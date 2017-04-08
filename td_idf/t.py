"""
Try to run the file with command: spark-submit tf-idf.py <arguments>.
For more info, visit: http://spark.apache.org/docs/latest/quick-start.html#self-contained-applications.
"""

from pyspark import SparkContext
import sys

sc = SparkContext("local", "TF-IDF")
sc.setLogLevel("ERROR")

print("TF-IDF Assignment")
file = sc.textFile("data.txt").cache()
print("File has " + str(file.count()) + " lines.")
print("Passed arguments " + str(sys.argv))

sc.stop()
