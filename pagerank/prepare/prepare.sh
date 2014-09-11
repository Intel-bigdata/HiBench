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

echo "========== preparing pagerank data=========="
# configure
DIR=`cd $bin/../; pwd`
. "${DIR}/../bin/sparkbench-config.sh"
. "${DIR}/conf/configure.sh"

# compress check
# compress
if [ $COMPRESS -eq 1 ]; then
    COMPRESS_OPT="-c ${COMPRESS_CODEC}"
else
    COMPRESS_OPT=""
fi

# path check
$HADOOP_EXECUTABLE dfs -rmr $INPUT_HDFS_DIR || true
$HADOOP_EXECUTABLE dfs -rmr $INPUT_HDFS || true

# generate data
#DELIMITER=\t
OPTION="-t pagerank \
        -b ${PAGERANK_BASE_HDFS} \
        -n ${PAGERANK_INPUT} \
        -m ${NUM_MAPS} \
        -r ${NUM_REDS} \
        -p ${PAGES} \
        -o text"
#       -d ${DELIMITER} \
$HADOOP_EXECUTABLE jar ${DATATOOLS} HiBench.DataGen ${OPTION} ${COMPRESS_OPT}
result=$?
if [ $result -ne 0 ]
then
    echo "ERROR: Hadoop job failed to run successfully." 
    exit $result
fi
${SPARK_HOME}/bin/spark-submit --class com.intel.sparkbench.datagen.convert.PagerankConvert --master ${SPARK_MASTER} ${SPARKBENCH_JAR} ${INPUT_HDFS_DIR}/edges ${INPUT_HDFS} ${PARALLEL}
result=$?
if [ $result -ne 0 ]
then
    echo "ERROR: covert job failed to run successfully." 
    exit $result
fi
