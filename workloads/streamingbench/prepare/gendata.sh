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

#SRC_DIR="$DIR/../../src/streambench/datagen"

#. "${SRC_DIR}/conf/configure.sh"
#. "${DIR}/prepare/genSeedDataset.sh" $textdataset_recordsize_factor
data_dir=$DATA_GEN_DIR/src/main/resources

#echo "=========begin gen stream data========="
echo "Topic:$topicName dataset:$app records:$records kafkaBrokers:$brokerList mode:$mode data_dir:$data_dir"

if [ "$mode" == "push" ]; then
	records=$(($records/4))
	for i in `seq 4`; do      {
		$JAVA_BIN -Xmx256M -server -XX:+UseCompressedOops -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:+CMSScavengeBeforeRemark -XX:+DisableExplicitGC -Djava.awt.headless=true  -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false  -Dkafka.logs.dir=bin/../logs -cp :${DATA_GEN_DIR}/lib/kafka-clients-0.8.1.jar:${DATA_GEN_DIR}/target/datagen-0.0.1.jar com.intel.PRCcloud.StartNew $app $topicName $brokerList $records $data_dir
	    }& 
	done
	wait
else
	$JAVA_BIN -Xmx256M -server -XX:+UseCompressedOops -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:+CMSScavengeBeforeRemark -XX:+DisableExplicitGC -Djava.awt.headless=true  -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false  -Dkafka.logs.dir=bin/../logs -cp :${DATA_GEN_DIR}/lib/kafka-clients-0.8.1.jar:${DATA_GEN_DIR}/target/datagen-0.0.1.jar com.intel.PRCcloud.StartPeriodic $app $topicName $brokerList $recordPerInterval $intervalSpan $totalRound $data_dir
fi

show_bannar finish
