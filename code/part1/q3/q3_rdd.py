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

spark = SparkSession.builder.appName("query3-rdd").getOrCreate()
sc = spark.sparkContext

#1o map -> ( movie_id, (vote, 1) )
#reduceByKey -> ( movie_id, (vote_sum, vote_count) )
#vote_avg -> ( movie_id, vote_avg )
vote_avg = \
    sc.textFile("hdfs://master:9000/files/ratings.csv"). \
    map(  lambda x : ( x.split(",")[1], (float(x.split(",")[2]) , 1) )  ). \
    reduceByKey( lambda x, y: (x[0] + y[0], x[1] + y[1]) ). \
    map( lambda x : (x[0], x[1][0]/x[1][1]) )

#genres -> ( movie_id, genre )
genres = \
    sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
    map(  lambda x : ( x.split(",")[0], x.split(",")[1] )  )

#map -> ( genre, (vote_avg, 1) )
#reduceByKey -> ( genre, (vote_avg_sum, vote_avg_count) )
#vote_avg_count = movie_count because each movie has its own vote_avg
#vote_avg -> ( genre, vote_avg, movie_count )
res = genres.join(vote_avg). \
    map( lambda x : (x[1][0], (x[1][1], 1)) ). \
    reduceByKey( lambda x, y: (x[0] + y[0], x[1] + y[1]) ). \
    map( lambda x : (x[0], (x[1][0]/x[1][1], x[1][1])) ). \
    sortByKey()

res_1 = res.collect()


print("-------------------------------------------------")
print("Query 3 (RDD) Outputs")
print("OUTPUT FORMAT: (genre, (vote_avg, movie_count))")
print("\n")
for i in res_1:
    print(i)
print("\n")
res_2 = res.map(list_to_csv_str).coalesce(1).saveAsTextFile("hdfs://master:9000/outputs/q3_rdd.csv")
end_time = time.time()
total_time = end_time - start_time
print("Total execution time for Query 3 (Map Reduce Query) is: " + str(total_time) + " sec\n")
print("-------------------------------------------------")
