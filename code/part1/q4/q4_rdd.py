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

spark = SparkSession.builder.appName("query4-rdd").getOrCreate()
sc = spark.sparkContext


def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

# one line of x is equal to (Id, Title, Summary, Date, Duration, Cost, Profit, Popularity)
# Date is a timestamp so it has format year-month-dayTtime+Z
def mapMovies(x):
    movie_id = x[0]

    SummaryWords = x[2].split(" ")
    count = 0
    for i in SummaryWords:
        count += 1
    
    y = int(x[3].split("-")[0])
    if  y >= 2000 and y <= 2004: 
        year = "2000-2004"
    elif y >= 2005 and y <= 2009: 
        year = "2005-2009"
    elif y >= 2010 and y <= 2014: 
        year = "2010-2014"
    else: 
        year = "2015-2019"

    return (movie_id, (year, count))


# genres = ( movie_id, genre ) only for Ids that genre == "Drama"
genres = \
    sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
    map( lambda x : (x.split(",")[0], x.split(",")[1]) ). \
    filter( lambda x : x[1] == "Drama" )


# 1o map -> (Id, Title, Summary, Date, Duration, Cost, Profit, Popularity)
# filter -> if release year is not between 2000 - 2019 and if there is no summary the line is discarded
# mapMovies -> ( movie_id, (5_year_period, number_of_summary_words) )
movies = \
    sc.textFile("hdfs://master:9000/files/movies.csv"). \
    map( lambda x : split_complex(x) ). \
    filter( lambda x : (x[2] != '' and x[3].split("-")[0] != '' and int(x[3].split("-")[0]) >= 2000 and int(x[3].split("-")[0]) <= 2019 ) ). \
    map( lambda x : mapMovies(x) )


# join -> ( movie_id, ((5_year_period, number_of_summary_words), genre) )
# map -> ( 5_year_period, (number_of_summary_words, 1) )
# reduceByKey -> ( 5_year_period, (words_sum, words_count) )
# res -> ( 5_year_period, summary_words_avg )
res = movies.join(genres). \
    map( lambda x : (x[1][0][0], (x[1][0][1], 1)) ). \
    reduceByKey( lambda x, y: (x[0] + y[0], x[1] + y[1]) ). \
    map( lambda x : (x[0], x[1][0]/x[1][1]) ). \
    sortByKey()

res_1 = res.collect()


print("-------------------------------------------------")
print("Query 4 (RDD) Outputs")
print("OUTPUT FORMAT: (five_year_period, average_summary_words_for_drama_movies)")
print("\n")
for i in res_1:
    print(i)
print("\n")
res_2 = res.map(list_to_csv_str).coalesce(1).saveAsTextFile("hdfs://master:9000/outputs/q4_rdd.csv")
end_time = time.time()
total_time = end_time - start_time
print("Total execution time for Query 4 (Map Reduce Query) is: " + str(total_time) + " sec\n")
print("-------------------------------------------------")
