from pyspark.sql import SparkSession
import time

start_time = time.time()

spark = SparkSession.builder.appName("query3-sql-csv").getOrCreate()

ratings = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/files/ratings.csv")
genres = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/files/movie_genres.csv")

ratings.registerTempTable("ratings")
genres.registerTempTable("genres")

sqlString = \
	"select g._c1 as genre, avg(r.voteAvg) as mean_genre_rate, count(r.Id) as movies_in_genre " + \
	"from (select _c1 as Id, avg(_c2) as voteAvg from ratings group by _c1) as r, genres as g " + \
	"where r.Id == g._c0 " + \
	"group by g._c1 " + \
	"order by g._c1"

res = spark.sql(sqlString)

res.coalesce(1).write.mode('overwrite').format('com.databricks.spark.csv').save("hdfs://master:9000/outputs/q3_sql_csv.csv")

res.show()
end_time = time.time()
total_time = end_time - start_time
print("Total execution time for Query 3 (Spark SQL - csv format) is: " + str(total_time) + " sec\n")

