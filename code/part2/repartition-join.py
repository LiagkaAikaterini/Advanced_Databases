import csv
import sys
from pyspark.sql import SparkSession
import time
import io
spark = SparkSession.builder.appName("repartition-join").getOrCreate()

sc = spark.sparkContext



def list_to_csv_str(x):
    """Given a list of strings, returns a properly-csv-formatted string."""
    output = io.StringIO("")
    csv.writer(output).writerow(x)
    return output.getvalue().strip() # remove extra newline

def join(list):
        table1 = []
        table2 = []
        for v in list:
                if v[0] == 'table1':
                        table1.append(v)
                elif v[0] == 'table2':
                        table2.append(v)
        return [(v[1], w[1]) for v in table1 for w in table2]

# The user must give the file path of each file as input with the index of the key for each one

# For example:
# spark-submit repartition_join.py "hdfs://master:9000/movies/movie-genres-100.csv" 0 "hdfs://master:9000/movies/ratings.csv" 1
# to join movie-genres-100 with ratings on movie id


file1 = sys.argv[1]

key1_index = int(sys.argv[2])

file2 = sys.argv[3]

key2_index = int(sys.argv[4])

table1 = \
        sc.textFile(file1). \
        map(lambda x : (x.split(','))). \
        map(lambda x : (x.pop(key1_index), ('table1', x)))

table2 = \
        sc.textFile(file2). \
        map(lambda x : (x.split(','))). \
        map(lambda x : (x.pop(key2_index), ('table2', x)))

start = time.time()

joined_table = \
        table1.union(table2). \
        groupByKey(). \
        flatMapValues(lambda x : join(x)). \
        map(lambda x : [x[0]] +  x[1][0] + x[1][1]).map(list_to_csv_str).coalesce(1).saveAsTextFile("hdfs://master:9000/outputs/repartition100.csv");

end = time.time()



print("\n\n Time for join: %.4f seconds\n\n"%(end-start))
