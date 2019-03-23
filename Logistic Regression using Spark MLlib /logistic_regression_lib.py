from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithSGD


def readPointBatch(lines):

    values = [float(line) for line in lines.split(' ')]
    return (values[0], values[1:])


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: wordcount <input_file>"
        exit(-1)
        
    sc = SparkContext(appName="LogisticRegressionWithSGD")

    points = sc.textFile(sys.argv[1]).map(readPointBatch)
    iterations = int(sys.argv[2])
    for i in range(iterations):
        print ("For iteration %i" % (i + 1))
        mod = LogisticRegressionWithSGD.train(points, iterations)
    print("Final weights: " + str(mod.weights))
    sc.stop()