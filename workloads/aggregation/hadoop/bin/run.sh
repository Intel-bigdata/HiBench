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

enter_bench HadoopAggregate ${workload_root} ${workload_folder}
show_bannar start

ensure-hivebench-release

cp ${WORKLOAD_FOLDER}/hive/bin/hive $HIVE_HOME/bin
USERVISITS_AGGRE="uservisits_aggre"
USERVISITS_AGGRE_FILE="uservisits_aggre.hive"

SUBDIR=$1
if [ -n "$SUBDIR" ]; then
  OUTPUT_HDFS=$OUTPUT_HDFS"/"$SUBDIR
  USERVISITS_AGGRE=$USERVISITS_AGGRE"_"$SUBDIR
  USERVISITS_AGGRE_FILE=$USERVISITS_AGGRE_FILE"."$SUBDIR
fi

# path check
rmr-hdfs /user/hive/warehouse/$USERVISITS_AGGRE

# pre-running
echo "USE DEFAULT;" > ${WORKLOAD_FOLDER}/hive-benchmark/$USERVISITS_AGGRE_FILE
echo "set $CONFIG_MAP_NUMBER=$NUM_MAPS;" >> ${WORKLOAD_FOLDER}/hive-benchmark/$USERVISITS_AGGRE_FILE
echo "set $CONFIG_REDUCER_NUMBER=$NUM_REDS;" >> ${WORKLOAD_FOLDER}/hive-benchmark/$USERVISITS_AGGRE_FILE
echo "set hive.stats.autogather=false;" >> ${WORKLOAD_FOLDER}/hive-benchmark/$USERVISITS_AGGRE_FILE


if [ $COMPRESS -eq 1 ]; then
  echo "set hive.exec.compress.output=true;" >> ${WORKLOAD_FOLDER}/hive-benchmark/$USERVISITS_AGGRE_FILE
  if [ "x"$HADOOP_VERSION == "xhadoop1" ]; then
    echo "set mapred.output.compress=true;" >> ${WORKLOAD_FOLDER}/hive-benchmark/$USERVISITS_AGGRE_FILE
    echo "set mapred.output.compression.type=BLOCK;" >> ${WORKLOAD_FOLDER}/hive-benchmark/$USERVISITS_AGGRE_FILE
    echo "set mapred.output.compression.codec=${COMPRESS_CODEC};" >> ${WORKLOAD_FOLDER}/hive-benchmark/$USERVISITS_AGGRE_FILE
  else
    echo "set mapreduce.jobtracker.address=ignorethis" >> ${WORKLOAD_FOLDER}/hive-benchmark/$USERVISITS_AGGRE_FILE
    echo "set hive.exec.show.job.failure.debug.info=false" >> ${WORKLOAD_FOLDER}/hive-benchmark/$USERVISITS_AGGRE_FILE
    echo "set mapreduce.map.output.compress=true;" >> ${WORKLOAD_FOLDER}/hive-benchmark/$USERVISITS_AGGRE_FILE
    echo "set mapreduce.map.output.compress.codec=${COMPRESS_CODEC};" >> ${WORKLOAD_FOLDER}/hive-benchmark/$USERVISITS_AGGRE_FILE
    echo "set mapreduce.fileoutputformat.compress.type=BLOCK;" >> ${WORKLOAD_FOLDER}/hive-benchmark/$USERVISITS_AGGRE_FILE
  fi
fi

echo "DROP TABLE uservisits;" >> ${WORKLOAD_FOLDER}/hive-benchmark/$USERVISITS_AGGRE_FILE
echo "DROP TABLE uservisits_aggre;" >> ${WORKLOAD_FOLDER}/hive-benchmark/$USERVISITS_AGGRE_FILE
echo "CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS SEQUENCEFILE LOCATION '$INPUT_HDFS/uservisits';">> ${WORKLOAD_FOLDER}/hive-benchmark/$USERVISITS_AGGRE_FILE
cat ${WORKLOAD_FOLDER}/hive-benchmark/uservisits_aggre.template >> ${WORKLOAD_FOLDER}/hive-benchmark/$USERVISITS_AGGRE_FILE

sed -i -e "s/uservisits\>/uservisits_${1}/1" ${WORKLOAD_FOLDER}/hive-benchmark/$USERVISITS_AGGRE_FILE
sed -i -e "s/uservisits_aggre/${USERVISITS_AGGRE}/g" ${WORKLOAD_FOLDER}/hive-benchmark/$USERVISITS_AGGRE_FILE

if [ "x"$HADOOP_VERSION == "xhadoop2" ]; then
  SIZE=`grep "BYTES_DATA_GENERATED=" ${WORKLOAD_FOLDER}/$TMPLOGFILE | sed 's/BYTES_DATA_GENERATED=//'`
else
  SIZE=$($HADOOP_EXECUTABLE job -history $INPUT_HDFS/uservisits | grep 'HiBench.Counters.*|BYTES_DATA_GENERATED')
  SIZE=${SIZE##*|}
  SIZE=${SIZE//,/}
fi
START_TIME=`timestamp`

# run bench
$HIVE_HOME/bin/hive -f ${WORKLOAD_FOLDER}/hive-benchmark/$USERVISITS_AGGRE_FILE
END_TIME=`timestamp`

gen_report ${START_TIME} ${END_TIME} ${SIZE}
show_bannar finish
leave_bench


#$HADOOP_EXECUTABLE $RMDIR_CMD $OUTPUT_HDFS/$USERVISITS_AGGRE
#$HADOOP_EXECUTABLE fs -mkdir -p $OUTPUT_HDFS
#$HADOOP_EXECUTABLE fs -cp /user/hive/warehouse/$USERVISITS_AGGRE $OUTPUT_HDFS

