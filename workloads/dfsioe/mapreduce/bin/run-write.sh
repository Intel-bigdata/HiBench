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

enter_bench HadoopDfsioe-write ${workload_root} ${workload_folder}
show_bannar start

#path check
rmr-hdfs ${OUTPUT_HDFS} || true

# pre-running
SIZE=`dir_size $INPUT_HDFS`
OPTION="-write -nrFiles ${WT_NUM_OF_FILES} -fileSize ${WT_FILE_SIZE} -bufferSize 4096 -plotInteval 1000 -sampleUnit m -sampleInteval 200 -sumThreshold 0.5 -tputReportTotal -Dtest.build.data=${INPUT_HDFS}"

OLD_HADOOP_OPTS=${HADOOP_OPTS:-}
export HADOOP_OPTS="${HADOOP_OPTS:-} -Dtest.build.data=${INPUT_HDFS} "

MONITOR_PID=`start-monitor`
START_TIME=`timestamp`

#run benchmark
run-hadoop-job ${DATATOOLS} org.apache.hadoop.fs.dfsioe.TestDFSIOEnh              \
    -Dmapreduce.map.java.opts="-Dtest.build.data=${INPUT_HDFS} $MAP_JAVA_OPTS"    \
    -Dmapreduce.reduce.java.opts="-Dtest.build.data=${INPUT_HDFS} $RED_JAVA_OPTS" \
    ${OPTION} -resFile ${WORKLOAD_RESULT_FOLDER}/result_write.txt                 \
    -tputFile ${WORKLOAD_RESULT_FOLDER}/throughput_write.csv

# post-running
END_TIME=`timestamp`
export HADOOP_OPTS="$OLD_HADOOP_OPTS"
stop-monitor $MONITOR_PID

gen_report ${START_TIME} ${END_TIME} ${SIZE}
show_bannar finish
leave_bench