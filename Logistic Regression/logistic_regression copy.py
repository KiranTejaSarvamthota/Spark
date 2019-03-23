from collections import namedtuple
from math import exp
from os.path import realpath
import sys
import numpy as np
from pyspark import SparkContext
D = 16  # Number of dimensions
def readPointBatch(iterator):
    strs = list(iterator)
    matrix = np.zeros((len(strs), D + 1))
    for i in xrange(len(strs)):
        matrix[i] = np.fromstring(strs[i].replace(',', ' '), dtype=np.float32, sep=' ')
    return [matrix]

if __name__ == "__main__":

    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: logistic_regression <file> <iterations>"
        exit(-1)

    sc = SparkContext(appName="PythonLR")
    points = sc.textFile(sys.argv[1]).mapPartitions(readPointBatch).cache()
    iterations = int(sys.argv[2])

    w = 2 * np.random.ranf(size=D) - 1
    print "Initial w: " + str(w)

    def gradient(matrix, w):
        Y = matrix[:, 0]    # point labels (first column of input file)
        X = matrix[:, 1:]   # point coordinates
	z = np.dot(X,w)
        return ((1.0 / (1.0 + np.exp(-Y * z)) - 1.0) * Y * X.T).sum(1)

    def add(x, y):
        x += y
        return x

    for i in range(iterations):
        print "On iteration %i" % (i + 1)
        w -= points.map(lambda m: gradient(m, w)).reduce(add)

    print "Final w: " + str(w)

    sc.stop()
