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

echo "========== running pagerank bench =========="
# configure
DIR=`cd $bin/../; pwd`
. "${DIR}/../bin/hibench-config.sh"
. "${DIR}/conf/configure.sh"

# compress check
if [ $COMPRESS -eq 1 ]
then
    COMPRESS_OPT="-Dmapred.output.compress=true \
    -Dmapred.output.compression.codec=$COMPRESS_CODEC"
else
    COMPRESS_OPT="-Dmapred.output.compress=false"
fi

# path check
$HADOOP_HOME/bin/hadoop dfs -rmr $TEMP_HDFS
$HADOOP_HOME/bin/hadoop dfs -rmr $OUTPUT_HDFS

# pre-running
SIZE=`$HADOOP_HOME/bin/hadoop fs -dus $INPUT_HDFS | awk '{ print $2 }'`

if [ $BLOCK -eq 0 ]
then
    OPTION="${COMPRESS_OPT} ${INPUT_HDFS}/edges ${OUTPUT_HDFS} ${PAGES} ${NUM_REDS} ${NUM_ITERATIONS} nosym new"
else
    OPTION="${COMPRESS_OPT} ${OUTPUT_HDFS} ${PAGES} ${NUM_REDS} ${NUM_ITERATIONS} ${BLOCK_WIDTH}"
fi

START_TIME=`timestamp`

# run bench
if [ $BLOCK -eq 0 ]
then
    $HADOOP_HOME/bin/hadoop jar ${DIR}/pegasus-2.0.jar pegasus.PagerankNaive $OPTION
else
    $HADOOP_HOME/bin/hadoop jar ${DIR}/pegasus-2.0.jar pegasus.PagerankInitVector ${COMPRESS_OPT} ${OUTPUT_HDFS}/pr_initvector ${PAGES} ${NUM_REDS}
    $HADOOP_HOME/bin/hadoop dfs -rmr ${OUTPUT_HDFS}/pr_input

    $HADOOP_HOME/bin/hadoop dfs -rmr ${OUTPUT_HDFS}/pr_iv_block
    $HADOOP_HOME/bin/hadoop jar ${DIR}/pegasus-2.0.jar pegasus.matvec.MatvecPrep ${COMPRESS_OPT} ${OUTPUT_HDFS}/pr_initvector ${OUTPUT_HDFS}/pr_iv_block ${PAGES} ${BLOCK_WIDTH} ${NUM_REDS} s makesym
    $HADOOP_HOME/bin/hadoop dfs -rmr ${OUTPUT_HDFS}/pr_initvector

    $HADOOP_HOME/bin/hadoop dfs -rmr ${OUTPUT_HDFS}/pr_edge_colnorm
    $HADOOP_HOME/bin/hadoop jar ${DIR}/pegasus-2.0.jar pegasus.PagerankPrep ${COMPRESS_OPT} ${INPUT_HDFS}/edges ${OUTPUT_HDFS}/pr_edge_colnorm ${NUM_REDS} makesym

    $HADOOP_HOME/bin/hadoop dfs -rmr ${OUTPUT_HDFS}/pr_edge_block
    $HADOOP_HOME/bin/hadoop jar ${DIR}/pegasus-2.0.jar pegasus.matvec.MatvecPrep ${COMPRESS_OPT} ${OUTPUT_HDFS}/pr_edge_colnorm ${OUTPUT_HDFS}/pr_edge_block ${PAGES} ${BLOCK_WIDTH} ${NUM_REDS} null nosym
    $HADOOP_HOME/bin/hadoop dfs -rmr ${OUTPUT_HDFS}/pr_edge_colnorm

    $HADOOP_HOME/bin/hadoop jar ${DIR}/pegasus-2.0.jar pegasus.PagerankBlock ${OPTION}
fi

# post-running
END_TIME=`timestamp`
gen_report "PAGERANK" ${START_TIME} ${END_TIME} ${SIZE} >> ${HIBENCH_REPORT}
