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
from IOCommon import IOCommon


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: terasort <HDFS_INPUT> <HDFS_OUTPUT>"
        exit(-1)
    sc = SparkContext(appName="PythonTeraSort")
    reducer = int(IOCommon.getProperty("hibench.default.shuffle.parallelism"))

    version_api = IOCommon.getProperty("hibench.hadoop.version")
    # load
    if version_api=="hadoop1":
        lines = sc.textFile(sys.argv[1], 1).map(lambda x: (x[:10], x[10:]))
    elif version_api == "hadoop2":
        lines = sc.newAPIHadoopFile(sys.argv[1], "org.apache.hadoop.examples.terasort.TeraInputFormat",
                                    "org.apache.hadoop.io.Text", "org.apache.hadoop.io.Text")

    # sort
    sortedCount = lines.sortByKey(lambda x: x, numPartitions = reducer)

    # save
    if version_api=="hadoop1":
        lines = sortedCount.map(lambda x: x[0] + x[1])
        sortedCount.saveAsTextFile(sys.argv[2])
    elif version_api == "hadoop2":
        sortedCount.saveAsNewAPIHadoopFile(sys.argv[2], "org.apache.hadoop.examples.terasort.TeraOutputFormat",
                                           "org.apache.hadoop.io.Text", "org.apache.hadoop.io.Text")

    sc.stop()
