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

echo "========== running kmeans bench =========="
# configure
DIR=`cd $bin/../; pwd`
. "${DIR}/../bin/hibench-config.sh"
. "${DIR}/conf/configure.sh"

if [ $COMPRESS -eq 1 ]; then
    COMPRESS_OPT="-Dmapred.output.compress=true
    -Dmapred.output.compression.codec=$COMPRESS_CODEC"
else
    COMPRESS_OPT="-Dmapred.output.compress=false"
fi

# path check
$HADOOP_EXECUTABLE dfs -rmr ${OUTPUT_HDFS}

# pre-running
SSIZE=$($HADOOP_EXECUTABLE job -history $INPUT_SAMPLE | grep 'HiBench.Counters.*|BYTES_DATA_GENERATED')
SSIZE=${SSIZE##*|}
SSIZE=${SSIZE//,/}
CSIZE=`dir_size $INPUT_CLUSTER`
SIZE=$(($SSIZE+$CSIZE))
OPTION="$COMPRESS_OPT -i ${INPUT_SAMPLE} -c ${INPUT_CLUSTER} -o ${OUTPUT_HDFS} -x ${MAX_ITERATION} -ow -cl -cd 0.5 -dm org.apache.mahout.common.distance.EuclideanDistanceMeasure -xm mapreduce"
START_TIME=`timestamp`

# run bench
${MAHOUT_HOME}/bin/mahout kmeans  ${OPTION}
result=$?
if [ $? -ne 0 ]
then
    echo "ERROR: Hadoop job failed to run successfully." 
    exit $result
fi

# post-running
END_TIME=`timestamp`
gen_report "KMEANS" ${START_TIME} ${END_TIME} ${SIZE}

