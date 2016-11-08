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
workload_folder=`cd "${workload_folder}"; pwd`
root_folder=${workload_folder}/../../..
root_folder=`cd "${root_folder}"; pwd`
common_folder=${root_folder}/src/streambench/common/target


KAFKA_FOLDER=/home/stream/mirror/kafka/kafka_2.11-0.8.2.2
ZK_HOST="sr502:2181,sr503:2181,sr504:2181"
NUM_OF_RECORDS=5000000
NUM_OF_THREADS=20
REPORT_PATH=/home/stream/mirror/spark_metrics

${KAFKA_FOLDER}/bin/kafka-topics.sh --zookeeper ${ZK_HOST} --list

read -p "Please input the args: <topic>" topic

java -cp ${common_folder}/streaming-bench-common-5.0-SNAPSHOT-jar-with-dependencies.jar \
     com.intel.hibench.streambench.common.metrics.MetricsReader \
     ${ZK_HOST} ${topic} ${REPORT_PATH} ${NUM_OF_RECORDS} ${NUM_OF_THREADS}