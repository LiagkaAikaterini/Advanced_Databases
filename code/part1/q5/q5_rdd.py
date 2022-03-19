from pyspark.sql import SparkSession
import time
from io import StringIO
import csv, io

def list_to_csv_str(x):
    """Given a list of strings, returns a properly-csv-formatted string."""
    output = io.StringIO("")
    csv.writer(output).writerow(x)
    return output.getvalue().strip() # remove extra newline

start_time = time.time()

spark = SparkSession.builder.appName("query5-rdd").getOrCreate()
sc = spark.sparkContext

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

#( ratingSum, (userIds) )
def findMaxRatingUser(x, y):
    ratingCountX = x[0]
    ratingCountY = y[0]
    if ratingCountX > ratingCountY:
        return x
    elif ratingCountY > ratingCountX:
        return y
    else:   
        return (x[0], x[1]+y[1])

def mapMultipleUsers(x):
    res = []
    genre = x[0]
    ratingCount = x[1][0]
    for userId in x[1][1]:
        output_item = ( userId, (ratingCount, genre) )
        res.append(output_item)
    return res

# ( rating, popul, title )
def minRating(x, y):
    ratingX = x[0]
    ratingY = y[0]
    if ratingX < ratingY:
        return x
    elif ratingY < ratingX:
        return y
    else:
        popularityX = x[1]
        popularityY = y[1]
        if popularityX < popularityY:
            return y
        else:
            return x

def maxRating(x, y):
    ratingX = x[0]
    ratingY = y[0]
    if ratingX > ratingY:
        return x
    elif ratingY > ratingX:
        return y
    else:
        popularityX = x[1]
        popularityY = y[1]
        if popularityX < popularityY:
            return y
        else:
            return x

# ( movie_id, (title, popul) )
movies = \
    sc.textFile("hdfs://master:9000/files/movies.csv"). \
    map( lambda x : (split_complex(x)[0], (split_complex(x)[1], float(split_complex(x)[7])) ) )

# ( movie_id, (user_id, rating) )
ratings = \
    sc.textFile("hdfs://master:9000/files/ratings.csv"). \
    map( lambda x : ( x.split(",")[1], (x.split(",")[0], float(x.split(",")[2])) ) )

# ( movie_id, genre )
genres = \
    sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
    map( lambda x : (x.split(",")[0], x.split(",")[1]) )

#( user_id, (movie_id, rating) )
helperRatings = ratings. \
    map( lambda x : (x[1][0], (x[0], x[1][1])) )


# (movieId, ( (userId, rating), genre) )
#    0           100     101      11
# 1o map -> ( (genre, userId), ratingCount==1 )
#                00      01        1
# 2o map -> ( genre, (ratingCount, (userIds)) ) -> userId is tuple so + can work in findMaxRatingUser
#               0        10         11
# flatMap -> ( userId, (ratingCount, genre)) -> maybe duplicates with different userIds if max_rating_count in one genre is the same for multiple users

# I have found the wanted user_ids and the max_rating_count for each genre category

# join -> (userId, ( (ratingCount, wantedgenre), (movie_id, rating)) )
#             0           100       101            110        111
# map -> (movie_id, (userId, ratingCount, wantedgenre, rating) )
#            0         10       11           12          13
# join -> ( movie_id, ( (userId, ratingCount, wantedgenre, rating), genre) ) 
#              0          100      101            102        103     11             
# filter -> wantedGenre == genre or DISCARD, so we can search the ratings for the approriate category (genre)
# join -> ( movie_id, (    ( (userId, ratingCount, wantedgenre, rating), genre),   (title, popul) ) )
#              0               1000      1001         1002      1003      101        110    111
# map -> ( (genre, user_id, ratingCount), (rating, popul, title) )
#             00      01        02           10     11     12
ratingsFormated = ratings.join(genres). \
    map( lambda x: ( (x[1][1], x[1][0][0]), 1 ) ). \
    reduceByKey( lambda x,y : x+y ). \
    map( lambda x : (x[0][0], (x[1], (x[0][1], )) ) ). \
    reduceByKey( lambda x, y: findMaxRatingUser(x, y) ). \
    flatMap(lambda x : mapMultipleUsers(x)). \
    join(helperRatings). \
    map ( lambda x: (x[1][1][0], (x[0], x[1][0][0], x[1][0][1], x[1][1][1])) ). \
    join(genres). \
    filter( lambda x : (x[1][1] == x[1][0][2]) ). \
    join(movies). \
    map( lambda x : ( (x[1][0][0][2], x[1][0][0][0], x[1][0][0][1]), (x[1][0][0][3], x[1][1][1], x[1][1][0]) ) )


minRatings = ratingsFormated. \
    reduceByKey( lambda x, y : minRating(x, y) )

maxRatings = ratingsFormated. \
    reduceByKey( lambda x, y : maxRating(x, y) )

# join -> ( (genre, user_id, ratingCount), ((ratingMin, populMin, titleMin) , (ratingMax, populMax, titleMax)) )
#              00      01        02             100       101        102          110         111      112      
# map to make Genre the key so then we will be able to sort by genre
# map -> res = ( genre, user_id, ratingCount, titleMax, ratingMax, titleMin, ratingMin )
res = minRatings.join(maxRatings). \
    map( lambda x : ( x[0][0], (x[0][1], x[0][2], x[1][1][2], x[1][1][0], x[1][0][2], x[1][0][0]) )). \
    sortByKey(). \
    map ( lambda x : (x[0], x[1][0], x[1][1], x[1][2], x[1][3], x[1][4], x[1][5]) )

res_1 = res.collect()

print("-------------------------------------------------")
print("Query 5 (RDD) Outputs")
print("OUTPUT FORMAT: ( genre, user_id, rating_count, title_of_Max, rating_Max, title_of_Min, rating_Min )")
print("\n")
for i in res_1:
    print(i)
print("\n")
res_2 = res.map(list_to_csv_str).coalesce(1).saveAsTextFile("hdfs://master:9000/outputs/q5_rdd.csv")
end_time = time.time()
total_time = end_time - start_time
print("Total execution time for Query 5 (Map Reduce Query) is: " + str(total_time) + " sec\n")
print("-------------------------------------------------")
