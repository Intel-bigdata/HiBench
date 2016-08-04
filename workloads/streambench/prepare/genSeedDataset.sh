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

#This script takes t argument scaleFactor

workload_folder=`dirname "$0"`
workload_folder=`cd "$workload_folder"; pwd`
workload_root=${workload_folder}/..
. "${workload_root}/../../bin/functions/load-bench-config.sh"

# generate seed data1 by hive
enter_bench HadoopPrepareDatafile1 ${workload_root} ${workload_folder}
show_bannar start

rmr-hdfs $STREAMING_DATA_DIR || true
echo -e "${On_Blue}Pages:${PAGES}, USERVISITS:${USERVISITS}${Color_Off}"

OPTION="-t hive \
        -b ${STREAMING_DATA_DIR} \
        -n ${STREAMING_DATA1_NAME} \
        -m ${NUM_MAPS} \
        -r ${NUM_REDS} \
        -p ${PAGES} \
        -v ${USERVISITS}"

START_TIME=`timestamp`
run-hadoop-job ${DATATOOLS} HiBench.DataGen ${OPTION} ${DATATOOLS_COMPRESS_OPT}
END_TIME=`timestamp`
SIZE="0"

show_bannar finish
leave_bench

# generate seed data2 by kmeans
enter_bench HadoopPrepareDatafile2 ${workload_root} ${workload_folder}
show_bannar start

rmr-hdfs $STREAMING_DATA2_SAMPLE_DIR || true
OPTION="-sampleDir ${STREAMING_DATA2_SAMPLE_DIR} -clusterDir ${STREAMING_DATA2_CLUSTER_DIR} -numClusters ${NUM_OF_CLUSTERS} -numSamples ${NUM_OF_SAMPLES} -samplesPerFile ${SAMPLES_PER_INPUTFILE} -sampleDimension ${DIMENSIONS} -textOutput"
export HADOOP_CLASSPATH=`${MAHOUT_HOME}/bin/mahout classpath`
export_withlog HADOOP_CLASSPATH
run-hadoop-job ${DATATOOLS} org.apache.mahout.clustering.kmeans.GenKMeansDataset -D hadoop.job.history.user.location=${STREAMING_DATA2_SAMPLE_DIR} ${OPTION} 
END_TIME=`timestamp`

show_bannar finish
leave_bench

