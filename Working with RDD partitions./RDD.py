import sys
from operator import add
from pyspark import SparkContext


if len(sys.argv) != 1:
    print >> sys.stderr, "Usage: wordcount <input_file> <output_file>"
    exit(-1)
sc = SparkContext(appName="PythonWordCount")

# x = sc.parallelize([("a",1), ("a", 1)])
# x.mapValues(lambda x:len(x)).collect()



rdd = sc.parallelize([("a", 1), ("a", 1), ("a", 1),("b", 1), ("b", 1), ("b", 1),("c", 1), ("c", 1), ("c", 1),("d", 1), ("d", 1), ("a", 1),("b", 1), ("c", 1), ("d", 1)])
rdd.countByKey()
print("The length of the list for each letter")
print(rdd.countByKey())

# lines = sc.textFile(sys.argv[1])
# counts = lines.flatMap(lambda line: line.strip().split(' ')).map(lambda word: (word, 1)).reduceByKey(add)
# counts.saveAsTextFile(sys.argv[2])
sc.stop()
