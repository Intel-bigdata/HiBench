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

def parseVector(line):
    return np.array([float(x) for x in line.split(' ')])

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: bayes <file> <numFeatures>"
        exit(-1)
    sc = SparkContext(appName="PythonNaiveBayes")
    filename = sys.argv[1]
    numFeatures = int(sys.argv[2])
#    print "###### Load svm file", filename
    examples = MLUtils.loadLibSVMFile(sc, filename, numFeatures = numFeatures)
    examples.cache()

    # FIXME: need randomSplit!
    training = examples.sample(False, 0.8, 2)
    test = examples.sample(False, 0.2, 2)

#    numTraining = training.count()
#    numTest = test.count()
#    print " numTraining = %d, numTest = %d." % (numTraining, numTest)
    model = NaiveBayes.train(training, 1.0)

    model_share=sc.broadcast(model)
    predictionAndLabel = test.map( lambda x: (x.label, model_share.value.predict(x.features)))
#    prediction = model.predict(test.map( lambda x: x.features ))
#    predictionAndLabel = prediction.zip(test.map( lambda x:x.label ))
    accuracy = predictionAndLabel.filter(lambda x: x[0] == x[1]).count() * 1.0 / numTest

    println("Test accuracy = %s." % accuracy)

