#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Copied from spark's example
#

"""
A K-means clustering program using MLlib.

This example requires NumPy (http://www.numpy.org/).
"""

import sys

import numpy as np
from pyspark import SparkContext
from pyspark.mllib.clustering import KMeans


def parseVector(line):
    vector = eval(line[1])
    return np.array([vector.get(x,0.0) for x in range(max(vector)+1)])


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print >> sys.stderr, "Usage: kmeans <file> <k> <max_iteration>"
        exit(-1)
    sc = SparkContext(appName="PythonKMeans")
    lines = sc.sequenceFile(sys.argv[1], "org.apache.hadoop.io.LongWritable", "org.apache.mahout.math.VectorWritable",
                            valueConverter="com.intel.sparkbench.datagen.pythonconverter.MahoutVectorToStringConverter")
    data = lines.map(parseVector)
    k = int(sys.argv[2])
    max_iterations = int(sys.argv[3])
    model = KMeans.train(data, k, max_iterations)
    print "Final centers: " + str(model.clusterCenters)
