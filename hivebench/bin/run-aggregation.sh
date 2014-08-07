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

echo "========== running hive-aggregate bench =========="
# configure
DIR=`cd $bin/../; pwd`
. "${DIR}/../bin/hibench-config.sh"
. "${DIR}/conf/configure.sh"

# path check
rm -rf ${DIR}/metastore_db
rm -rf ${DIR}/TempStatsStore
$HADOOP_EXECUTABLE $RMDIR_CMD /user/hive/warehouse/uservisits_aggre
#$HADOOP_EXECUTABLE $RMDIR_CMD /tmp

# pre-running
echo "USE DEFAULT;" > $DIR/hive-benchmark/uservisits_aggre.hive
echo "set $CONFIG_MAP_NUMBER=$NUM_MAPS;" >> $DIR/hive-benchmark/uservisits_aggre.hive
echo "set $CONFIG_REDUCER_NUMBER=$NUM_REDS;" >> $DIR/hive-benchmark/uservisits_aggre.hive
echo "set hive.stats.autogather=false;" >> $DIR/hive-benchmark/uservisits_aggre.hive


if [ $COMPRESS -eq 1 ]; then
  echo "set hive.exec.compress.output=true;" >> $DIR/hive-benchmark/uservisits_aggre.hive
  if [ "x"$HADOOP_VERSION == "xhadoop1" ]; then
    echo "set mapred.output.compress=true;" >> $DIR/hive-benchmark/uservisits_aggre.hive
    echo "set mapred.output.compression.type=BLOCK;" >> $DIR/hive-benchmark/uservisits_aggre.hive
    echo "set mapred.output.compression.codec=${COMPRESS_CODEC};" >> $DIR/hive-benchmark/uservisits_aggre.hive
  else
    echo "set mapreduce.jobtracker.address=ignorethis" >> $DIR/hive-benchmark/uservisits_aggre.hive
    echo "set hive.exec.show.job.failure.debug.info=false" >> $DIR/hive-benchmark/uservisits_aggre.hive
    echo "set mapreduce.map.output.compress=true;" >> $DIR/hive-benchmark/uservisits_aggre.hive
    echo "set mapreduce.map.output.compress.codec=${COMPRESS_CODEC};" >> $DIR/hive-benchmark/uservisits_aggre.hive
    echo "set mapreduce.fileoutputformat.compress.type=BLOCK;" >> $DIR/hive-benchmark/uservisits_aggre.hive
  fi
fi

echo "DROP TABLE uservisits;" >> $DIR/hive-benchmark/uservisits_aggre.hive
echo "DROP TABLE uservisits_aggre;" >> $DIR/hive-benchmark/uservisits_aggre.hive
echo "CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS SEQUENCEFILE LOCATION '$INPUT_HDFS/uservisits';">> $DIR/hive-benchmark/uservisits_aggre.hive
cat $DIR/hive-benchmark/uservisits_aggre.template >> $DIR/hive-benchmark/uservisits_aggre.hive

if [ "x"$HADOOP_VERSION == "xhadoop2" ]; then
  SIZE=`grep "BYTES_DATA_GENERATED=" ${DIR}/$TMPLOGFILE | sed 's/BYTES_DATA_GENERATED=//'`
else
  SIZE=$($HADOOP_EXECUTABLE job -history $INPUT_HDFS/uservisits | grep 'HiBench.Counters.*|BYTES_DATA_GENERATED')
  SIZE=${SIZE##*|}
  SIZE=${SIZE//,/}
fi
START_TIME=`timestamp`

# run bench
echo $HIVE_HOME/bin/hive -f $DIR/hive-benchmark/uservisits_aggre.hive
$HIVE_HOME/bin/hive -f $DIR/hive-benchmark/uservisits_aggre.hive

# post-running
END_TIME=`timestamp`
gen_report "HIVEAGGR" ${START_TIME} ${END_TIME} ${SIZE}

$HADOOP_EXECUTABLE $RMDIR_CMD $OUTPUT_HDFS
$HADOOP_EXECUTABLE fs -mkdir $OUTPUT_HDFS
$HADOOP_EXECUTABLE $RMDIR_CMD $OUTPUT_HDFS
$HADOOP_EXECUTABLE fs -cp /user/hive/warehouse/uservisits_aggre $OUTPUT_HDFS

