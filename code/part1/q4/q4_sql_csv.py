from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType
import time

start_time = time.time()

spark = SparkSession.builder.appName("query4-sql-csv").getOrCreate()

def yearPeriodSpecified(yearString): 
    y = int(yearString)

    if  y >= 2000 and y <= 2004:
        year = "2000-2004"
    elif y >= 2005 and y <= 2009:
        year = "2005-2009"
    elif y >= 2010 and y <= 2014:
        year = "2010-2014"
    else:
        year = "2015-2019"

    return year

movies = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/files/movies.csv")
genres = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/files/movie_genres.csv")

movies.registerTempTable("movies")
genres.registerTempTable("genres")

spark.udf.register("summaryLength", lambda x: len(x.split()), IntegerType())
spark.udf.register("quinquennium", yearPeriodSpecified, StringType())

sqlString = \
        "select quinquennium(year(m._c3)) as five_year_period, avg(summaryLength(m._c2)) as average_summary_words " + \
        "from (movies as m join genres as g on m._c0 == g._c0) " + \
        "where m._c0 == g._c0 and g._c1 == 'Drama' and m._c2 != '' and year(m._c3) >= 2000 and year(m._c3) <= 2019 " + \
        "group by five_year_period " + \
        "order by five_year_period"

res = spark.sql(sqlString)

res.coalesce(1).write.mode('overwrite').format('com.databricks.spark.csv').save("hdfs://master:9000/outputs/q4_sql_csv.csv")

res.show()
end_time = time.time()
total_time = end_time - start_time
print("Total execution time for Query 4 (Spark SQL - csv format) is: " + str(total_time) + " sec\n")
