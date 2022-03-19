from pyspark.sql import SparkSession
import time

start_time = time.time()

spark = SparkSession.builder.appName("query3-sql-parquet").getOrCreate()

ratings = spark.read.parquet("hdfs://master:9000/files/ratings.parquet")
genres = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")

ratings.registerTempTable("ratings")
genres.registerTempTable("genres")

sqlString = \
        "select g._c1 as genre, avg(r.voteAvg) as mean_genre_rate, count(r.Id) as movies_in_genre " + \
        "from (select _c1 as Id, avg(_c2) as voteAvg from ratings group by _c1) as r, genres as g " + \
        "where r.Id == g._c0 " + \
        "group by g._c1 " + \
        "order by g._c1"

res = spark.sql(sqlString)

res.coalesce(1).write.mode('overwrite').format('com.databricks.spark.csv').save("hdfs://master:9000/outputs/q3_sql_parquet.csv")

res.show()
end_time = time.time()
total_time = end_time - start_time
print("Total execution time for Query 3 (Spark SQL - parquet format) is: " + str(total_time) + " sec\n")
