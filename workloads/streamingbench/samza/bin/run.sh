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

#set -u
#bin=`dirname "$0"`
#bin=`cd "$bin";pwd`
#DIR=`cd $bin/../; pwd`
#SRC_DIR="$DIR/../../../src/streambench/samza"
#. "${SRC_DIR}/conf/configure.sh"
#echo "=========start samza benchmark $benchName========="
SRC_DIR=${workload_root}/../../src/streambench/samzabench/

# prepare samza environment

if [ ! -d $SRC_DIR/target ]; then
  echo "Please run 'bin/build-all.sh' first."
  exit 1
fi

SAMZA_PLAYGROUND=${WORKLOAD_RESULT_FOLDER}/samza

mkdir -p $SAMZA_PLAYGROUND 2> /dev/null
tar zxf $SRC_DIR/target/*.tar.gz -C ${SAMZA_PLAYGROUND}


CLASSPATH=$HADOOP_CONF_DIR
for file in $SAMZA_PLAYGROUND/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

#if [ -z "$JAVA_HOME" ]; then
#  JAVA="java"
#else
#  JAVA="$JAVA_HOME/bin/java"
#fi

JAVA_OPTS="-Dlog4j.configuration=file://${SAMZA_PLAYGROUND}/lib/log4j.xml -Dsamza.log.dir=${SAMZA_PLAYGROUND}"
configFactory=org.apache.samza.config.factories.PropertiesConfigFactory
#configFile=$SRC_DIR/conf/$benchName.properties

#rm -f $SRC_DIR/conf/.properties
#cp $configFile $SRC_DIR/conf/.properties
#cat $SRC_DIR/conf/common.properties >> $SRC_DIR/conf/.properties

CMD="${JAVA_BIN} ${JAVA_OPTS} -cp ${CLASSPATH} org.apache.samza.job.JobRunner --config-factory=${configFactory} --config-path=file://${SAMZA_PROP_CONF}"
echo ${CMD}

START_TIME=`timestamp`
${CMD}
END_TIME=`timestamp`

gen_report ${START_TIME} ${END_TIME} 0 # FIXME, size should be throughput
show_bannar finish
