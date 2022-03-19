from io import StringIO
import csv, io
from pyspark.sql import SparkSession
import time
#filter(lambda x : (x[5] == 0) or (x[6] == 0) or (int(x[3].split("")[0:3]) < 2000)).map(get_profit)
#return (year, (title, profit))
#.map(get_profit).reduceByKey(max)
spark = SparkSession.builder.appName("query1-rdd").getOrCreate()

sc = spark.sparkContext

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

def get_profit(pieces):
    cost = float(pieces[5])
    income = float(pieces[6])
    title = pieces[1]
    year = int(pieces[3][0:4]) if (pieces[3][0:3]) else 0
    if (cost != 0):
        profit = (income - cost) * 100 / cost
    if(cost != 0 and income !=0 and year >= 2000):
        return (year,(title, profit))
    else: return (year,(title, 0))

def list_to_csv_str(x):
    """Given a list of strings, returns a properly-csv-formatted string."""
    output = io.StringIO("")
    csv.writer(output).writerow(x)
    return output.getvalue().strip() # remove extra newline


def get_max(a, b):
    a1 = a[0]
    a2 = a[1]
    b1 = b[0]
    b2 = b[1]
    if (a2 > b2 ):
        return  a
    else:
        return b
start = time.time()
res = sc.textFile("hdfs://master:9000/files/movies.csv").map(split_complex).map(get_profit).filter(lambda a : a[1][1] != 0).reduceByKey(get_max).sortByKey().map(lambda a: [a[0]] + [a[1][0]] +[a[1][1]]).map(list_to_csv_str).coalesce(1).saveAsTextFile("hdfs://master:9000/outputs/q1_rdd.csv");
end  = time.time()
print("\n\n Time for query: %.4f seconds\n\n"%(end-start))

