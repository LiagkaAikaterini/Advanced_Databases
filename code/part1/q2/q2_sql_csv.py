from pyspark.sql import SparkSession
import time

start_time = time.time()

spark = SparkSession.builder.appName("query2-sql-csv").getOrCreate()

ratings = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/files/ratings.csv")

spark.conf.set("spark.sql.crossJoin.enabled", "true")

ratings.registerTempTable("ratings")

sqlString = \
'select (cast(count(distinct s._c0) as float)/cast(count(distinct r._c0) as float))*100 as percentage from (select _c0, avg(_c2) as average from ratings group by _c0 having average>3) as s full outer join ratings as r on s._c0==r._c0'

res = spark.sql(sqlString)

res.coalesce(1).write.mode('overwrite').format('com.databricks.spark.csv').save("hdfs://master:9000/outputs/q2_sql_csv.csv")
res.show()
end_time = time.time()
total_time = end_time - start_time
out_str = "Total execution time for Query 2 (Spark SQL - csv format) is: " + str(total_time) + " sec\n"
print(out_str)
