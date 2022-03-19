from io import StringIO
import csv, io
from pyspark.sql import SparkSession
import time

def list_to_csv_str(x):
    """Given a list of strings, returns a properly-csv-formatted string."""
    output = io.StringIO("")
    csv.writer(output).writerow(x)
    return output.getvalue().strip() # remove extra newline

start_time = time.time()

spark = SparkSession.builder.appName("query2-rdd").getOrCreate()
sc = spark.sparkContext

percentage = \
sc.textFile("hdfs://master:9000/files/ratings.csv"). \
map (lambda x : (x.split(",")[0], (float(x.split(",")[2]),1))). \
reduceByKey( lambda x, y: (x[0] + y[0], x[1]+y[1])). \
map(lambda x : (1,(1,1)) if (x[1][0]/x[1][1] > 3) else (1,(1,0))). \
reduceByKey(lambda x , y:  (x[0]+y[0], x[1]+y[1])). \
map(lambda x : (1, (x[1][1]/x[1][0])*100))


percentage_1 = percentage.collect()
print("-------------------------------------------------")
print("Query 2 (RDD) Outputs")
print("OUTPUT FORMAT: (1, percentage)")
print("\n")
print("Pecentage is: ", percentage_1)
print("\n")
percentage = percentage.map(list_to_csv_str).coalesce(1).saveAsTextFile("hdfs://master:9000/outputs/q2_rdd.csv")
end_time = time.time()
total_time = end_time - start_time
print("Total execution time for Query 2 (Map Reduce Query) is: " + str(total_time) + " sec\n")
print("-------------------------------------------------")
