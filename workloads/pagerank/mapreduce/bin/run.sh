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

enter_bench HadoopPagerank ${workload_root} ${workload_folder}
show_bannar start

rmr-hdfs $OUTPUT_HDFS || true

SIZE=`dir_size $INPUT_HDFS`

if [ $BLOCK -eq 0 ]
then
    OPTION="${COMPRESS_OPT} ${INPUT_HDFS}/edges ${OUTPUT_HDFS} ${PAGES} ${NUM_REDS} ${NUM_ITERATIONS} nosym new"
else
    OPTION="${COMPRESS_OPT} ${OUTPUT_HDFS} ${PAGES} ${NUM_REDS} ${NUM_ITERATIONS} ${BLOCK_WIDTH}"
fi

MONITOR_PID=`start-monitor`
START_TIME=`timestamp`

# run bench
if [ $BLOCK -eq 0 ]
then
    run-hadoop-job ${PEGASUS_JAR} pegasus.PagerankNaive $OPTION
else
    run-hadoop-job ${PEGASUS_JAR} pegasus.PagerankInitVector ${COMPRESS_OPT} ${OUTPUT_HDFS}/pr_initvector ${PAGES} ${NUM_REDS}
    rmr-hdfs ${OUTPUT_HDFS}/pr_input

    rmr-hdfs ${OUTPUT_HDFS}/pr_iv_block
    run-hadoop-job ${PEGASUS_JAR} pegasus.matvec.MatvecPrep ${COMPRESS_OPT} ${OUTPUT_HDFS}/pr_initvector ${OUTPUT_HDFS}/pr_iv_block ${PAGES} ${BLOCK_WIDTH} ${NUM_REDS} s makesym
    rmr-hdfs ${OUTPUT_HDFS}/pr_initvector

    rmr-hdfs ${OUTPUT_HDFS}/pr_edge_colnorm
    run-hadoop-job ${PEGASUS_JAR} pegasus.PagerankPrep ${COMPRESS_OPT} ${INPUT_HDFS}/edges ${OUTPUT_HDFS}/pr_edge_colnorm ${NUM_REDS} makesym

    rmr-hdfs ${OUTPUT_HDFS}/pr_edge_block
    run-hadoop-job ${PEGASUS_JAR} pegasus.matvec.MatvecPrep ${COMPRESS_OPT} ${OUTPUT_HDFS}/pr_edge_colnorm ${OUTPUT_HDFS}/pr_edge_block ${PAGES} ${BLOCK_WIDTH} ${NUM_REDS} null nosym
    rmr-hdfs ${OUTPUT_HDFS}/pr_edge_colnorm

    run-hadoop-job ${PEGASUS_JAR} pegasus.PagerankBlock ${OPTION}
fi

END_TIME=`timestamp`
stop-monitor $MONITOR_PID

gen_report ${START_TIME} ${END_TIME} ${SIZE}
show_bannar finish
leave_bench

