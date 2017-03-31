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
root_dir=${current_dir}/../../../../../
workload_config=${root_dir}/conf/workloads/micro/dfsioe.conf
. "${root_dir}/bin/functions/load_bench_config.sh"

enter_bench HadoopPrepareDFSIOE ${workload_config} ${current_dir}
show_bannar start

rmr_hdfs $INPUT_HDFS || true

MONITOR_PID=`start_monitor`
START_TIME=`timestamp`

run_hadoop_job ${DATATOOLS} org.apache.hadoop.fs.dfsioe.TestDFSIOEnh                \
    -Dmapreduce.map.java.opts=\"-Dtest.build.data=${INPUT_HDFS} ${MAP_JAVA_OPTS}\"    \
    -Dmapreduce.reduce.java.opts=\"-Dtest.build.data=${INPUT_HDFS} ${RED_JAVA_OPTS}\" \
    -Dtest.build.data=${INPUT_HDFS}                                                 \
    -write -skipAnalyze -nrFiles ${RD_NUM_OF_FILES} -fileSize ${RD_FILE_SIZE} -bufferSize 4096


END_TIME=`timestamp`
stop_monitor $MONITOR_PID

show_bannar finish
leave_bench

