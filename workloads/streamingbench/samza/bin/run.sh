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
. "${workload_root}/../../bin/functions/load-bench-config.sh"

enter_bench SamzaStreamingBench ${workload_root} ${workload_folder}
show_bannar start

SRC_DIR=${workload_root}/../../src/streambench/samzabench

printFullLog

# prepare samza environment

if [ ! -d $SRC_DIR/target ]; then
  echo "Please run 'bin/build-all.sh' first."
  exit 1
fi

SAMZA_PLAYGROUND=${WORKLOAD_RESULT_FOLDER}/samza

mkdir -p $SAMZA_PLAYGROUND 2> /dev/null
tar zxf $SRC_DIR/target/*.tar.gz -C ${SAMZA_PLAYGROUND}

configFactory=org.apache.samza.config.factories.PropertiesConfigFactory

# remove samza prefix and change "name    value" to "name=value" style in ${SAMZA_PROP_CONF}
cat ${SAMZA_PROP_CONF} | sed -r 's/samza\.//' | sed -r 's/\t+/=/' > ${SAMZA_PROP_CONF}.converted

# Upload samza package to HDFS
upload-to-hdfs $STREAMING_SAMZA_PACKAGE_LOCAL_PATH $STREAMING_SAMZA_PACKAGE_HDFS_PATH

$HADOOP_EXECUTABLE --config $HADOOP_CONF_DIR fs -put $STREAMING_SAMZA_PACKAGE_LOCAL_PATH $STREAMING_SAMZA_PACKAGE_HDFS_PATH

function samza-submit() {
    workload_name=$1
    CMD="$SAMZA_PLAYGROUND/bin/run-job.sh --config-factory=${configFactory} --config-path=file://${SAMZA_PROP_CONF}.converted"
    echo -e "${BGreen}Submit Samza Job: ${Green}$CMD${Color_Off}"
    execute_withlog $CMD
}

START_TIME=`timestamp`
. $SRC_DIR/scripts/$STREAMING_BENCHNAME.sh
END_TIME=`timestamp`

gen_report ${START_TIME} ${END_TIME} 0 # FIXME, size should be throughput
show_bannar finish
