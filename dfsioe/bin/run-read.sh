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
set -u 

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

echo "========== Running dfsioe-read bench =========="
# configure
DIR=`cd $bin/../; pwd`
. "${DIR}/../bin/hibench-config.sh"
. "${DIR}/conf/configure.sh"

# path check
$HADOOP_EXECUTABLE dfs -rmr ${INPUT_HDFS}/io_read
$HADOOP_EXECUTABLE dfs -rmr ${INPUT_HDFS}/_*

# pre-running
#SIZE=`$HADOOP_EXECUTABLE fs -dus ${INPUT_HDFS} | grep -o [0-9]*`
SIZE=`dir_size $INPUT_HDFS`
#OPTION="-read -skipAnalyze -nrFiles ${RD_NUM_OF_FILES} -fileSize ${RD_FILE_SIZE} -bufferSize 131072 -plotInteval 1000 -sampleUnit m -sampleInteval 200 -sumThreshold 0.5"
OPTION="-read -nrFiles ${RD_NUM_OF_FILES} -fileSize ${RD_FILE_SIZE} -bufferSize 131072 -plotInteval 1000 -sampleUnit m -sampleInteval 200 -sumThreshold 0.5 -tputReportTotal"
START_TIME=`timestamp`

# run bench
${HADOOP_EXECUTABLE} jar ${DATATOOLS} org.apache.hadoop.fs.dfsioe.TestDFSIOEnh ${OPTION} -resFile ${DIR}/result_read.txt -tputFile ${DIR}/throughput_read.csv
if [ $? -ne 0 ]
then
    echo "ERROR: Hadoop job failed to run successfully." 
    exit 1
fi

# post-running
END_TIME=`timestamp`
gen_report "DFSIOE-READ" ${START_TIME} ${END_TIME} ${SIZE}
