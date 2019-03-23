import sys
from operator import add
from pyspark import SparkContext


if len(sys.argv) != 2:
    print >> sys.stderr, "Usage: wordcountext <input_file>"
    exit(-1)
sc = SparkContext(appName="PythonWordCountExt")
lines = sc.textFile(sys.argv[1])
counts = lines.flatMap(lambda x: x.split()).map(lambda x: (x.lower(),1)).reduceByKey(lambda x,y:x+y)
def comp(x,y):
	if x[1]<y[1]:
		return y
	else:
		return x  
mostFreq=counts.reduce(comp) 
print mostFreq
sc.stop()