from pyspark import SparkContext
import sys
if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: logmining <file>"
        exit(-1)
sc = SparkContext(appName="logmining")
text_file = sc.textFile(sys.argv[1])
errors = text_file.filter(lambda line: "error" in line)
errors.cache()
count = errors.count()
print "Total number of errors %d",count
browser_error = errors.filter(lambda line: "log" in line).count()
print "Mozilla error count :%d",browser_error
