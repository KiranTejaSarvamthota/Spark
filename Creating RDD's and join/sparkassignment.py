import sys
from operator import add
from pyspark import SparkContext
sc = SparkContext(appName="Spark assignment1")
if len(sys.argv) != 4:
    print >> sys.stderr, "Usage: wordcount <input_file> <input_file> <output_file>"
    exit(-1)

data1 = sc.textFile(sys.argv[1])
spark_lines = data1.filter(lambda line: "Spark" in line)
count1 = spark_lines.flatMap(lambda x: x.strip().split(' ')).map(lambda x: (x, 1)).reduceByKey(add)
data2 = sc.textFile(sys.argv[2])
lines2 = data2.filter(lambda line: "Spark" in line)
count2 = lines2.flatMap(lambda x: x.strip().split(' ')).map(lambda x: (x, 1)).reduceByKey(add)
joined_Data = count2.join(count1)
joined_Data.saveAsTextFile(sys.argv[3])