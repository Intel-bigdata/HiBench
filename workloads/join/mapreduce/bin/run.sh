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

workload_folder=`dirname "$0"`
workload_folder=`cd "$workload_folder"; pwd`
workload_root=${workload_folder}/../..
. "${workload_root}/../../bin/functions/load-bench-config.sh"

enter_bench HadoopJoin ${workload_root} ${workload_folder}
show_bannar start

ensure-hivebench-release

cp ${HIVEBENCH_TEMPLATE}/bin/hive $HIVE_HOME/bin
RANKINGS_USERVISITS_JOIN_FILE="rankings_uservisits_join.hive"

# path check
rmr-hdfs $OUTPUT_HDFS

# prepare SQL
HIVEBENCH_SQL_FILE=${WORKLOAD_RESULT_FOLDER}/$RANKINGS_USERVISITS_JOIN_FILE

cat <<EOF > ${HIVEBENCH_SQL_FILE}
USE DEFAULT;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set ${MAP_CONFIG_NAME}=$NUM_MAPS;
set ${REDUCER_CONFIG_NAME}=$NUM_REDS;
set hive.stats.autogather=false;
EOF

#if [ $COMPRESS -eq 1 ]; then
#  echo "set hive.exec.compress.output=true;" >> ${WORKLOAD_FOLDER}/hive-benchmark/$RANKINGS_USERVISITS_JOIN_FILE
#  if [ "x"$HADOOP_VERSION == "xhadoop1" ]; then
#    echo "set mapred.output.compress=true;" >> ${WORKLOAD_FOLDER}/hive-benchmark/$RANKINGS_USERVISITS_JOIN_FILE
#    echo "set mapred.output.compression.type=BLOCK;" >> ${WORKLOAD_FOLDER}/hive-benchmark/$RANKINGS_USERVISITS_JOIN_FILE
#    echo "set mapred.output.compression.codec=${COMPRESS_CODEC};" >> ${WORKLOAD_FOLDER}/hive-benchmark/$RANKINGS_USERVISITS_JOIN_FILE
#  else
#    echo "set mapreduce.jobtracker.address=ignorethis" >> ${WORKLOAD_FOLDER}/hive-benchmark/$RANKINGS_USERVISITS_JOIN_FILE
#    echo "set hive.exec.show.job.failure.debug.info=false" >> ${WORKLOAD_FOLDER}/hive-benchmark/$RANKINGS_USERVISITS_JOIN_FILE
#    echo "set mapreduce.map.output.compress=true;" >> ${WORKLOAD_FOLDER}/hive-benchmark/$RANKINGS_USERVISITS_JOIN_FILE
#    echo "set mapreduce.map.output.compress.codec=${COMPRESS_CODEC};" >> ${WORKLOAD_FOLDER}/hive-benchmark/$RANKINGS_USERVISITS_JOIN_FILE
#    echo "set mapreduce.fileoutputformat.compress.type=BLOCK;" >> ${WORKLOAD_FOLDER}/hive-benchmark/$RANKINGS_USERVISITS_JOIN_FILE
#  fi
#fi

cat <<EOF >> ${HIVEBENCH_SQL_FILE}
DROP TABLE rankings;
DROP TABLE uservisits_copy;
DROP TABLE rankings_uservisits_join;
CREATE EXTERNAL TABLE rankings (pageURL STRING, pageRank INT, avgDuration INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS SEQUENCEFILE LOCATION '$INPUT_HDFS/rankings';
CREATE EXTERNAL TABLE uservisits_copy (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS SEQUENCEFILE LOCATION '$INPUT_HDFS/uservisits';
CREATE EXTERNAL TABLE rankings_uservisits_join ( sourceIP STRING, avgPageRank DOUBLE, totalRevenue DOUBLE) STORED AS SEQUENCEFILE LOCATION '$OUTPUT_HDFS/rankings_uservisits_join';
INSERT OVERWRITE TABLE rankings_uservisits_join SELECT sourceIP, avg(pageRank), sum(adRevenue) as totalRevenue FROM rankings R JOIN (SELECT sourceIP, destURL, adRevenue FROM uservisits_copy UV WHERE (datediff(UV.visitDate, '1999-01-01')>=0 AND datediff(UV.visitDate, '2000-01-01')<=0)) NUV ON (R.pageURL = NUV.destURL) group by sourceIP order by totalRevenue DESC limit 1;
EOF

# run bench
START_TIME=`timestamp`
$HIVE_HOME/bin/hive -f ${HIVEBENCH_SQL_FILE}
END_TIME=`timestamp`

sleep 3
SIZE=`dir_size $OUTPUT_HDFS`

gen_report ${START_TIME} ${END_TIME} ${SIZE}
show_bannar finish
leave_bench

