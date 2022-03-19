from pyspark.sql import SparkSession
import time

start_time = time.time()

spark = SparkSession.builder.appName("query5-sql-parquet").getOrCreate()

#(id, title, summary, date, duration, cost, profit, popularity)
movies = spark.read.parquet("hdfs://master:9000/files/movies.parquet")

#(id, genre)
genres = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")

#(user_id, id, rating, time)
ratings = spark.read.parquet("hdfs://master:9000/files/ratings.parquet")

movies.registerTempTable("movies")
genres.registerTempTable("genres")
ratings.registerTempTable("ratings")


users = \
        "(select count(r._c1) as RatingCount, r._c0 as userId, g._c1 as genre "+ \
        "from ratings as r, genres as g "+ \
        "where g._c0 == r._c1 "+ \
        "group by g._c1, r._c0 ) as users "

# works
wantedUsers = \
        "(select maxratingcount.maxRatingCount as count, maxratingcount.genreCategory as genre, users.userId as userId "+ \
                "from "+ users + \
        "join "+ \
                "(select max(users.RatingCount) as maxRatingCount, users.genre as genreCategory "+ \
                "from "+ users + \
                "group by genreCategory) as maxratingcount "+ \
        "on users.genre == maxratingcount.genreCategory and users.RatingCount == maxratingcount.maxRatingCount"+ \
        ") as categories "

#works
ratingMap = \
        "(select step.rating as rating, step.userId as userId, step.genre as genre, step.movieId as movieId, m._c7 as popularity, m._c1 as title "+ \
        "from "+ \
                        "(select r._c2 as rating, r._c0 as userId, g._c1 as genre, r._c1 as movieId "+ \
                        "from ratings as r "+ \
                "join " \
                        "genres as g "+ \
                "on g._c0 == r._c1) as step " + \
        "join "+ \
                "movies as m "+ \
        "on m._c0 == step.movieId) as ratingMap "

#works
minRatingsPerCategoryPerUser = \
        "(select min(r._c2) as minrating, r._c0 as userId, g._c1 as genre "+ \
        "from ratings as r join genres as g "+ \
        "on g._c0 == r._c1 "+ \
        "group by r._c0, g._c1) as allMinRatings "


#works
# for wanted (userId, genre) -> all movies with min rating
wantedMinRatings = \
        "(select categories.userId as userId, categories.genre as genre, allMinRatings.minrating as minRating, categories.count as mycount "+ \
        "from "+ \
        wantedUsers+ \
        "join "+ \
        minRatingsPerCategoryPerUser+ \
        "on categories.userId == allMinRatings.userId and categories.genre == allMinRatings.genre) as minR "


#choose movie from min rating movies
minRes = \
        "(select minRes.userId as userId, minRes.genre as genre, minRes.minRating as minRating, minRes.mycount as mycount, ratingMap.title as minTitle from "+ \
                "(select max(ratingMap.popularity) as pop, minR.userId as userId, minR.genre as genre, minR.minRating as minRating, minR.mycount as mycount from "+ \
                ratingMap+ \
                "join "+ \
                wantedMinRatings+\
                "on ratingMap.rating == minR.minrating and ratingMap.userId == minR.userId and ratingMap.genre == minR.genre "+ \
                "group by minR.userId, minR.genre, minR.minRating, minR.mycount) as minRes "+ \
        "join "+ \
        ratingMap+ \
        "on ratingMap.popularity == minRes.pop and ratingMap.rating == minRes.minrating and ratingMap.userId == minRes.userId and ratingMap.genre == minRes.genre) as myMin "

#works
maxRatingsPerCategoryPerUser = \
        "(select max(r._c2) as maxrating, r._c0 as userId, g._c1 as genre "+ \
        "from ratings as r join genres as g "+ \
        "on g._c0 == r._c1 "+ \
        "group by r._c0, g._c1) as allMaxRatings "


#works
# for wanted (userId, genre) -> all movies with max rating
wantedMaxRatings = \
        "(select categories.userId as userId, categories.genre as genre, allMaxRatings.maxrating as maxRating, categories.count as mycount "+ \
        "from "+ \
        wantedUsers+ \
        "join "+ \
        maxRatingsPerCategoryPerUser+ \
        "on categories.userId == allMaxRatings.userId and categories.genre == allMaxRatings.genre) as maxR "


#choose movie from max rating movies
maxRes = \
        "(select maxRes.userId as userId, maxRes.genre as genre, maxRes.maxRating as maxRating, maxRes.mycount as mycount, ratingMap.title as maxTitle from "+ \
                "(select max(ratingMap.popularity) as pop, maxR.userId as userId, maxR.genre as genre, maxR.maxRating as maxRating, maxR.mycount as mycount from "+ \
                ratingMap+ \
                "join "+ \
                wantedMaxRatings+\
                "on ratingMap.rating == maxR.maxrating and ratingMap.userId == maxR.userId and ratingMap.genre == maxR.genre "+ \
                "group by maxR.userId, maxR.genre, maxR.maxRating, maxR.mycount) as maxRes "+ \
        "join "+ \
        ratingMap+ \
        "on ratingMap.popularity == maxRes.pop and ratingMap.rating == maxRes.maxrating and ratingMap.userId == maxRes.userId and ratingMap.genre == maxRes.genre) as myMax "




sqlString = \
        "select myMin.genre as genre, myMin.userId as user_id, myMin.mycount as rating_count, myMax.maxTitle as max_rated_title, myMax.maxRating as max_rating, myMin.minTitle as min_rated_title, myMin.minRating as min_rating "+ \
        "from "+ \
                minRes + \
                "join " + \
                maxRes + \
                "on myMin.userId == myMax.userId and myMin.genre == myMax.genre "+ \
        "order by myMin.genre"

res = spark.sql(sqlString)

res.coalesce(1).write.mode('overwrite').format('com.databricks.spark.csv').save("hdfs://master:9000/outputs/q5_sql_parquet.csv")

res.show(30)
end_time = time.time()
total_time = end_time - start_time
print("Total execution time for Query 5 (Spark SQL - parquet format) is: " + str(total_time) + " sec\n")
