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
workload_config=${root_dir}/conf/workloads/streaming/wordcount.conf
. "${root_dir}/bin/functions/load_bench_config.sh"

enter_bench StreamingWordcountPrepare ${workload_config} ${current_dir}
show_bannar start

DATA_FILE1=${STREAMING_DATA1_DIR}/uservisits
DATA_FILE2=${STREAMING_DATA2_SAMPLE_DIR}

JVM_OPTS="-Xmx1024M -server -XX:+UseCompressedOops -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:+CMSScavengeBeforeRemark -XX:+DisableExplicitGC -Djava.awt.headless=true -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dkafka.logs.dir=bin/../logs -cp ${DATATOOLS}"

printFullLog

CMD="$JAVA_BIN $JVM_OPTS com.intel.hibench.datagen.streaming.DataGenerator $SPARKBENCH_PROPERTIES_FILES $DATA_FILE1 0 $DATA_FILE2 0"
echo -e "${BGreen}Sending streaming data to kafka, periodically: ${Green}$CMD${Color_Off}"
execute_withlog $CMD

show_bannar finish
