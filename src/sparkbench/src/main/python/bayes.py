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

"""
A naive bayes program using MLlib.

This example requires NumPy (http://www.numpy.org/).
"""

import sys

from pyspark import SparkContext
from pyspark.mllib.util import MLUtils
from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import Vectors
from pyspark.storagelevel import StorageLevel
from operator import add
from itertools import groupby
#
# Adopted from spark's doc: http://spark.apache.org/docs/latest/mllib-naive-bayes.html
#
def parseVector(line):
    return np.array([float(x) for x in line.split(' ')])

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: bayes <file>"
        exit(-1)
    sc = SparkContext(appName="PythonNaiveBayes")
    filename = sys.argv[1]


    data = sc.sequenceFile(filename, "org.apache.hadoop.io.Text", "org.apache.hadoop.io.Text")
    wordCount = data                                \
        .flatMap(lambda (key, doc):doc.split(" "))    \
        .map(lambda x:(x, 1))                                \
        .reduceByKey(add)

    wordSum = wordCount.map(lambda x:x[1]).reduce(lambda x,y:x+y)
    wordDict = wordCount.zipWithIndex()             \
        .map(lambda ((key, count), index): (key, (index, count*1.0 / wordSum)) )             \
        .collectAsMap()
    sharedWordDict = sc.broadcast(wordDict)

    # for each document, generate vector based on word freq
    def doc2vector(dockey, doc):
        # map to word index: freq
        # combine freq with same word
        docVector = [(key, sum((z[1] for z in values))) for key, values in
                     groupby(sorted([sharedWordDict.value[x] for x in doc.split(" ")],
                                    key=lambda x:x[0]),
                             key=lambda x:x[0])]

        (indices, values) = zip(*docVector)      # unzip
        label = float(dockey[6:])
        return label, indices, values

    vector = data.map( lambda (dockey, doc) : doc2vector(dockey, doc))

    vector.persist(StorageLevel.MEMORY_ONLY)
    d = vector.map( lambda (label, indices, values) : indices[-1] if indices else 0)\
              .reduce(lambda a,b:max(a,b)) + 1


#    print "###### Load svm file", filename
    #examples = MLUtils.loadLibSVMFile(sc, filename, numFeatures = numFeatures)
    examples = vector.map( lambda (label, indices, values) : LabeledPoint(label, Vectors.sparse(d, indices, values)))

    examples.cache()

    # FIXME: need randomSplit!
    training = examples.sample(False, 0.8, 2)
    test = examples.sample(False, 0.2, 2)

    numTraining = training.count()
    numTest = test.count()
    print " numTraining = %d, numTest = %d." % (numTraining, numTest)
    model = NaiveBayes.train(training, 1.0)

    model_share = sc.broadcast(model)
    predictionAndLabel = test.map( lambda x: (x.label, model_share.value.predict(x.features)))
#    prediction = model.predict(test.map( lambda x: x.features ))
#    predictionAndLabel = prediction.zip(test.map( lambda x:x.label ))
    accuracy = predictionAndLabel.filter(lambda x: x[0] == x[1]).count() * 1.0 / numTest

    print "Test accuracy = %s." % accuracy

