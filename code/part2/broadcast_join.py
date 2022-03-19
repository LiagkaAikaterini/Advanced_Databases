from pyspark.sql import SparkSession
import time
import io, csv

start_time = time.time()

spark = SparkSession.builder.appName("broadcast-join").getOrCreate()
sc = spark.sparkContext

def list_to_csv_str(x):
    """Given a list of strings, returns a properly-csv-formatted string."""
    output = io.StringIO("")
    csv.writer(output).writerow(x)
    return output.getvalue().strip() # remove extra newline
# (movie_id, genre) -> key = movie_id
#     0        1
def map_R(x):
    return ( x.split(",")[0], (x.split(",")[1]) )

# (movie_id, (user_id, rating, time)) -> key = movie_id
#     0         10       11     12
def map_L(x):
    return ( x.split(",")[1], (x.split(",")[0], x.split(",")[2], x.split(",")[3]) )


# in this function we persume that the second path leads to dataset R, smaller than the dataset L of first path (R < L)
def Broadcast_Join(L_path, R_path):
    # We are going to create the Hash Table from R, because we persumed its smaller

    # So retrieve the smaller dataset R from R_path
    # and create Hash Table -> (key, (values))
    R = \
        sc.textFile(R_path). \
        map( lambda x : map_R(x) ). \
        groupByKey(). \
        collectAsMap()

    # Broadcast the HashTable we created
    BrHashTable = sc.broadcast(R)

    # function for joining the two datasets
    def JoinFunc(x):
        # x is one row from the L dataset
        key = x[0]
        values = x[1]
        res = []
        if BrHashTable.value.get(key) == None:
            return res
        else:
            for record in BrHashTable.value.get(key):
                joined_line = ( key, (record, values) )
                res.append(joined_line)
            return res

    # Retrieve biggest dataset L from L_path and perform the join
    join_result = \
        sc.textFile(L_path). \
        map( lambda x : map_L(x) ). \
        flatMap( lambda x : JoinFunc(x) )

    return join_result


# Movie_Genres < Ratings so based on the algorithm : L = ratings and R = genres
# we also create appropriate map_R, map_L functions (used in Broadcast_Join function) so we do the correct mapping for this data
genres_path = "hdfs://master:9000/files/movie_genres_100_rows.csv"
ratings_path = "hdfs://master:9000/files/ratings.csv"

res = Broadcast_Join(ratings_path, genres_path).map(lambda x : [x[0]] + [x[1][0]] + list(x[1][1])).map(list_to_csv_str).coalesce(1).saveAsTextFile("hdfs://master:9000/outputs/broadcast100.csv");

print("-------------------------------------------------")
print("Broadcast Join Output")
end_time = time.time()
total_time = end_time - start_time
print("Total execution time for our implementation of Broadcast Join is: " + str(total_time) + " sec\n")
print("-------------------------------------------------")
