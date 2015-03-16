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
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: python_spark_sql_bench.py <workload_name> <SQL script file>"
        exit(-1)
    workload_name, sql_script = sys.argv[1], sys.argv[2]
    sc = SparkContext(appName=workload_name)
    sqlctx = SQLContext(sc)
    hc = HiveContext(sc)
    with open(sql_script) as f:
        for line in f.read().split(';'):
            line = line.strip()
            if line:
                hc.sql(line)
    sc.stop()
