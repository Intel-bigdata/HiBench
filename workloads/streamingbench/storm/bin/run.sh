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
echo $workload_root
. "${workload_root}/../../bin/functions/load-bench-config.sh"

enter_bench StormStreamingBench ${workload_root} ${workload_folder}
show_bannar start
#set -u
#bin=`dirname "$0"`
#bin=`cd "$bin";pwd`
#DIR=`cd $bin/../; pwd`
#SRC_DIR="$DIR/../../../src/streambench/stormbench"
#. "${SRC_DIR}/conf/configure.sh"
#echo "=========start storm benchmark $benchName========="

#benchArgs="$nimbus $nimbusAPIPort $zkHost $workerCount $spoutThreads $boltThreads $benchName $recordCount $topicName $consumer $readFromStart $ackon $nimbusContactInterval"
#if [ "$benchName" == "micro-sample"  ]; then
#  benchArgs="$benchArgs $prob"
#elif [ "$benchName" == "trident-sample"  ]; then
#  benchArgs="$benchArgs $prob"
#elif [ "$benchName" == "micro-grep"  ]; then
#  benchArgs="$benchArgs $pattern"
#elif [ "$benchName" == "trident-grep"  ]; then
#  benchArgs="$benchArgs $pattern"
#else
#  benchArgs="$benchArgs $separator $fieldIndex "
#fi

gen_report ${START_TIME} ${END_TIME} 0 # FIXME, size should be throughput
show_bannar finish

cd ${workload_folder}

START_TIME=`timestamp`
CMD="$STORM_BIN_HOME/storm jar ${STREAMBENCH_STORM_JAR} com.intel.PRCcloud.RunBench $SPARKBENCH_PROPERTIES_FILES"
echo $CMD
$CMD
END_TIME=`timestamp`

gen_report ${START_TIME} ${END_TIME} 0 # FIXME, size should be throughput
show_bannar finish
