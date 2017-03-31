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
workload_config=${root_dir}/conf/workloads/streaming/fixwindow.conf
. "${root_dir}/bin/functions/load_bench_config.sh"


# generate seed data1 by hive
enter_bench HadoopPrepareDatafile1 ${workload_config} ${current_dir}
show_bannar start

PAGES=120000
USERVISITS=1000000

rmr_hdfs $STREAMING_DATA_DIR || true
echo -e "${On_Blue}Pages:${PAGES}, USERVISITS:${USERVISITS}${Color_Off}"

OPTION="-t hive \
        -b ${STREAMING_DATA_DIR} \
        -n ${STREAMING_DATA1_NAME} \
        -m ${NUM_MAPS} \
        -r ${NUM_REDS} \
        -p ${PAGES} \
        -v ${USERVISITS}"

START_TIME=`timestamp`
run_hadoop_job ${DATATOOLS} HiBench.DataGen ${OPTION}
END_TIME=`timestamp`
SIZE="0"

show_bannar finish
leave_bench

# generate seed data2 by kmeans
enter_bench HadoopPrepareDatafile2 ${workload_config} ${current_dir}
show_bannar start

rmr_hdfs $STREAMING_DATA2_SAMPLE_DIR || true
OPTION="-sampleDir ${STREAMING_DATA2_SAMPLE_DIR} -clusterDir ${STREAMING_DATA2_CLUSTER_DIR} -numClusters 5 -numSamples 3000000 -samplesPerFile 600000 -sampleDimension 20 -textOutput"
run_hadoop_job ${DATATOOLS} org.apache.mahout.clustering.kmeans.GenKMeansDataset -D hadoop.job.history.user.location=${STREAMING_DATA2_SAMPLE_DIR} ${OPTION}
END_TIME=`timestamp`

show_bannar finish
leave_bench
