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

current_dir=`dirname "$0"`
current_dir=`cd "$current_dir"; pwd`
root_dir=${current_dir}/../../../../..
workload_config=${root_dir}/conf/workloads/websearch/pagerank.conf
. "${root_dir}/bin/functions/load_bench_config.sh"

enter_bench HadoopPagerank ${workload_config} ${current_dir}
show_bannar start

rmr_hdfs $OUTPUT_HDFS || true

SIZE=`dir_size $INPUT_HDFS`

if [ $BLOCK -eq 0 ]
then
    OPTION="${INPUT_HDFS}/edges ${OUTPUT_HDFS} ${PAGES} ${NUM_REDS} ${NUM_ITERATIONS} nosym new"
else
    OPTION=" ${OUTPUT_HDFS} ${PAGES} ${NUM_REDS} ${NUM_ITERATIONS} ${BLOCK_WIDTH}"
fi

MONITOR_PID=`start_monitor`
START_TIME=`timestamp`

# run bench
if [ $BLOCK -eq 0 ]
then
    run_hadoop_job ${PEGASUS_JAR} pegasus.PagerankNaive $OPTION
else
    run_hadoop_job ${PEGASUS_JAR} pegasus.PagerankInitVector  ${OUTPUT_HDFS}/pr_initvector ${PAGES} ${NUM_REDS}
    rmr_hdfs ${OUTPUT_HDFS}/pr_input

    rmr_hdfs ${OUTPUT_HDFS}/pr_iv_block
    run_hadoop_job ${PEGASUS_JAR} pegasus.matvec.MatvecPrep  ${OUTPUT_HDFS}/pr_initvector ${OUTPUT_HDFS}/pr_iv_block ${PAGES} ${BLOCK_WIDTH} ${NUM_REDS} s makesym
    rmr_hdfs ${OUTPUT_HDFS}/pr_initvector

    rmr_hdfs ${OUTPUT_HDFS}/pr_edge_colnorm
    run_hadoop_job ${PEGASUS_JAR} pegasus.PagerankPrep  ${INPUT_HDFS}/edges ${OUTPUT_HDFS}/pr_edge_colnorm ${NUM_REDS} makesym

    rmr_hdfs ${OUTPUT_HDFS}/pr_edge_block
    run_hadoop_job ${PEGASUS_JAR} pegasus.matvec.MatvecPrep  ${OUTPUT_HDFS}/pr_edge_colnorm ${OUTPUT_HDFS}/pr_edge_block ${PAGES} ${BLOCK_WIDTH} ${NUM_REDS} null nosym
    rmr_hdfs ${OUTPUT_HDFS}/pr_edge_colnorm

    run_hadoop_job ${PEGASUS_JAR} pegasus.PagerankBlock ${OPTION}
fi

END_TIME=`timestamp`
stop_monitor $MONITOR_PID

gen_report ${START_TIME} ${END_TIME} ${SIZE}
show_bannar finish
leave_bench

