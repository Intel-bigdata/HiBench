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
root_dir=${current_dir}/../../../../
workload_config=${root_dir}/conf/micro/wordcount.conf
. "${root_dir}/bin/functions/load-bench-config.sh"

enter_bench HadoopPrepareWordcount ${workload_config}
show_bannar start

rmr-hdfs $INPUT_HDFS || true
START_TIME=`timestamp`
echo "DataSize:"${DATASIZE}
echo "map num:" {$NUM_MAPS}

run-hadoop-job ${HADOOP_EXAMPLES_JAR} randomtextwriter \
    -D ${BYTES_TOTAL_NAME}=${DATASIZE} \
    -D ${MAP_CONFIG_NAME}=${NUM_MAPS} \
    -D ${REDUCER_CONFIG_NAME}=${NUM_REDS} \
    ${COMPRESS_OPT} \
    ${INPUT_HDFS}
#run-spark-job com.intel.sparkbench.datagen.RandomTextWriter $INPUT_HDFS ${DATASIZE}
END_TIME=`timestamp`

