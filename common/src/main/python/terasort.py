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

import sys

from pyspark import SparkContext


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: terasort <HDFS_INPUT> <HDFS_OUTPUT>"
        exit(-1)
    sc = SparkContext(appName="PythonTeraSort")
    reducer = int(SparkContext._jvm.java.lang.System.getProperty("sparkbench.reducer"))
    lines = sc.textFile(sys.argv[1], 1)
    sortedCount = lines.map(lambda x: (x[:10], x[10:])) \
        .sortByKey(lambda x: x, numPartitions = reducer).map(lambda x: x[0] + x[1])

    sortedCount.saveAsTextFile(sys.argv[2])
    sc.stop()
