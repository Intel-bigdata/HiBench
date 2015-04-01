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
        print >> sys.stderr, "Usage: scan <hdfs_in_file> <hdfs_out_file>"
        exit(-1)
    sc = SparkContext(appName="PythonScan")
    sqlctx = SQLContext(sc)
    hc = HiveContext(sc)

    hc.sql("DROP TABLE if exists uservisits")
    hc.sql("DROP TABLE if exists uservisits_copy")
    hc.sql("CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS SEQUENCEFILE LOCATION '%s/uservisits'" % sys.argv[1])
    hc.sql("CREATE EXTERNAL TABLE uservisits_copy (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS SEQUENCEFILE LOCATION '%s/uservisits_copy'" % sys.argv[2])
    hc.sql("INSERT OVERWRITE TABLE uservisits_copy SELECT * FROM uservisits")

    sc.stop()
