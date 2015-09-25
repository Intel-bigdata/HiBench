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

enter_bench StreamingBenchZkUtils ${workload_root} ${workload_folder}
show_bannar start
printFullLog

# operation type
op=${1:-ls}

# number of partitions
partitions=$(($YARN_NUM_EXECUTORS*$YARN_EXECUTOR_CORES))

# zkHost address:port
zkHost=$STREAMING_ZKADDR

# topic
topic=$STREAMING_TOPIC_NAME

# spark consumer name
consumer=$STREAMING_CONSUMER_GROUP

path=/consumers/$consumer/offsets/$topic

CMD="$JAVA_BIN -cp $STREAMING_ZKHELPER_JAR com.intel.hibench.streambench.zkHelper.ZKUtil $op $zkHost $path $partitions"
echo -e "${BGreen}Query ZooKeeper for topic offsets, params: ${BCyan}operation:${Cyan}$op ${BCyan}partitions:${Cyan}$partitions ${BCyan}zkHost:${Cyan}$zkHost ${BCyan}topic:${Cyan}$topic ${BCyan}consumer:${Cyan}$consumer ${BCyan}path:${Cyan}$path${Color_Off}"
echo -e "${BGreen}Run:${Green}$CMD${Color_Off}"
execute_withlog $CMD

show_bannar finish