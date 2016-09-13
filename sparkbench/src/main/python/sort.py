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
# Adopted from spark's example
#

import sys
import bisect

from pyspark import SparkContext
from pyspark.rdd import portable_hash

def sortByKeyWithHashedPartitioner(self, ascending=True, numPartitions=None, keyfunc=lambda x: x):
    """
    Sorts this RDD, which is assumed to consist of (key, value) pairs.
    # adopted from spark/s rdd.py implementation

    >>> tmp = [('a', 1), ('b', 2), ('1', 3), ('d', 4), ('2', 5)]
    >>> sortByKeyWithHashedPartitioner(sc.parallelize(tmp)).first()
    ('d', 4)
    >>> sortByKeyWithHashedPartitioner(sc.parallelize(tmp), True, 1).collect()
    [('1', 3), ('2', 5), ('a', 1), ('b', 2), ('d', 4)]
    >>> sortByKeyWithHashedPartitioner(sc.parallelize(tmp), True, 2).collect()
    [('1', 3), ('a', 1), ('2', 5), ('b', 2), ('d', 4)]
    >>> tmp2 = [('Mary', 1), ('had', 2), ('a', 3), ('little', 4), ('lamb', 5)]
    >>> tmp2.extend([('whose', 6), ('fleece', 7), ('was', 8), ('white', 9)])
    >>> sortByKeyWithHashedPartitioner(sc.parallelize(tmp2),True, 3, keyfunc=lambda k: k.lower()).collect()
    [('fleece', 7), ('had', 2), ('lamb', 5), ('white', 9),....('Mary', 1), ('was', 8), ('whose', 6)]
    """
    if numPartitions is None:
        numPartitions = self._defaultReducePartitions()

    def sortPartition(iterator):
        return iter(sorted(iterator, key=lambda (k, v): keyfunc(k), reverse=not ascending))

    if numPartitions == 1:
        if self.getNumPartitions() > 1:
            self = self.coalesce(1)
        return self.mapPartitions(sortPartition)

    def hashedPartitioner(k):
        return portable_hash(keyfunc(k)) % numPartitions

    return self.partitionBy(numPartitions, hashedPartitioner).mapPartitions(sortPartition, True)



if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: sort <HDFS_INPUT> <HDFS_OUTPUT>"
        exit(-1)
    sc = SparkContext(appName="PythonSort")
    hdfs_input, hdfs_output = sys.argv[1], sys.argv[2]

#    reducer = int(SparkContext._jvm.com.intel.sparkbench.IOCommonWrap.getProperty("hibench.default.shuffle.parallelism"))
    reducer = int(sc._conf.get("spark.default.parallelism", str(sc.defaultParallelism / 2))) # FIXME: use IOCommonWrap!
    lines = sc.textFile(hdfs_input, 1).map(lambda x:(x,1))
    sortedWords = sortByKeyWithHashedPartitioner(lines, numPartitions=reducer).map(lambda x:x[0])

    sortedWords.saveAsTextFile(hdfs_output)
    sc.stop()
