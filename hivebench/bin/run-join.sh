#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

echo "========== running hive-join bench =========="
# configure
DIR=`cd $bin/../; pwd`
. "${DIR}/../bin/hibench-config.sh"
. "${DIR}/conf/configure.sh"

# path check
rm -rf ${DIR}/metastore_db
rm -rf ${DIR}/TempStatsStore
$HADOOP_HOME/bin/hadoop fs -rmr /user/hive/warehouse/rankings_uservisits_join
$HADOOP_HOME/bin/hadoop fs -rmr /tmp

# pre-running
echo "USE DEFAULT;">$DIR/hive-benchmark/rankings_uservisits_join.hive
echo "set mapred.map.tasks=$NUM_MAPS;">>$DIR/hive-benchmark/rankings_uservisits_join.hive
echo "set mapred.reduce.tasks=$NUM_REDS;">>$DIR/hive-benchmark/rankings_uservisits_join.hive
echo "set hive.stats.autogather=false;">>$DIR/hive-benchmark/rankings_uservisits_join.hive

if [ $COMPRESS -eq 1 ]; then
    echo "set mapred.output.compress=true;">>$DIR/hive-benchmark/rankings_uservisits_join.hive
    echo "set hive.exec.compress.output=true;">>$DIR/hive-benchmark/rankings_uservisits_join.hive
    echo "set mapred.output.compression.type=BLOCK;">>$DIR/hive-benchmark/rankings_uservisits_join.hive
    echo "set mapred.output.compression.codec=${COMPRESS_CODEC};">>$DIR/hive-benchmark/rankings_uservisits_join.hive
fi

echo "DROP TABLE rankings;">>$DIR/hive-benchmark/rankings_uservisits_join.hive
echo "DROP TABLE uservisits;">>$DIR/hive-benchmark/rankings_uservisits_join.hive
echo "DROP TABLE rankings_uservisits_join;">>$DIR/hive-benchmark/rankings_uservisits_join.hive
echo "CREATE EXTERNAL TABLE rankings (pageURL STRING, pageRank INT, avgDuration INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS SEQUENCEFILE LOCATION '$INPUT_HDFS/rankings';">>$DIR/hive-benchmark/rankings_uservisits_join.hive
echo "CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS SEQUENCEFILE LOCATION '$INPUT_HDFS/uservisits/';">>$DIR/hive-benchmark/rankings_uservisits_join.hive
cat $DIR/hive-benchmark/rankings_uservisits_join.template>>$DIR/hive-benchmark/rankings_uservisits_join.hive

SIZE=`$HADOOP_HOME/bin/hadoop fs -dus $INPUT_HDFS | awk '{ print $2 }'`
START_TIME=`timestamp`

# run bench
$HIVE_HOME/bin/hive -f $DIR/hive-benchmark/rankings_uservisits_join.hive

# post-running
END_TIME=`timestamp`
gen_report "HIVEJOIN" ${START_TIME} ${END_TIME} ${SIZE} >> ${HIBENCH_REPORT}

$HADOOP_HOME/bin/hadoop fs -rmr $OUTPUT_HDFS/hive-join
$HADOOP_HOME/bin/hadoop fs -cp /user/hive/warehouse/rankings_uservisits_join $OUTPUT_HDFS/hive-join
