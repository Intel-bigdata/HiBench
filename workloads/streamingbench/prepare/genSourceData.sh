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
workload_root=${workload_folder}/..
. "${workload_root}/../../bin/functions/load-bench-config.sh"

enter_bench StreamingBenchPrepare ${workload_root} ${workload_folder}
show_bannar start

DATA_GEN_DIR=${workload_root}/../../src/streambench/datagen
DATA_FILE1=${STREAMING_DATA1_DIR}/uservisits
DATA_FILE2=${STREAMING_DATA2_SAMPLE_DIR}

#echo "=========begin gen stream data========="
#echo "Topic:$topicName dataset:$app records:$records kafkaBrokers:$brokerList mode:$mode data_dir:$data_dir"


JVM_OPTS="-Xmx256M -server -XX:+UseCompressedOops -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:+CMSScavengeBeforeRemark -XX:+DisableExplicitGC -Djava.awt.headless=true -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dkafka.logs.dir=bin/../logs -cp ${DATA_GEN_JAR}"

printFullLog

if [ "$STREAMING_DATAGEN_MODE" == "push" ]; then
	    CMD="$JAVA_BIN $JVM_OPTS com.intel.hibench.streambench.StartNew $SPARKBENCH_PROPERTIES_FILES $DATA_FILE1 0 $DATA_FILE2 0"
	    echo -e "${BGreen}Sending streaming data to kafka, concurrently: ${Green}$CMD${Color_Off}"
	    execute_withlog $CMD
else
    CMD="$JAVA_BIN $JVM_OPTS com.intel.hibench.streambench.StartPeriodic $SPARKBENCH_PROPERTIES_FILES $DATA_FILE1 0 $DATA_FILE2 0"
    echo -e "${BGreen}Sending streaming data to kafka, periodically: ${Green}$CMD${Color_Off}"
    execute_withlog $CMD
fi

show_bannar finish