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
from pyspark.sql import SQLContext, HiveContext

#
# ported from HiBench's hive bench
#
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: join <SQL script file>"
        exit(-1)
    sc = SparkContext(appName="PythonJoin")
    sqlctx = SQLContext(sc)
    hc = HiveContext(sc)
    with open(sys.argv[1]) as f:
        for line in f.read().split(';'):
            line = line.strip()
            if line:
                hc.sql(line)
    sc.stop()
