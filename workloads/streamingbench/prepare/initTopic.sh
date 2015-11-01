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

enter_bench StreamingBenchInitTopic ${workload_root} ${workload_folder}

KAFKA_TOPIC_BIN="$STREAMING_KAFKA_HOME/bin/kafka-topics.sh --zookeeper $STREAMING_ZKADDR"

show_bannar start

# delete all topics
topics=`$KAFKA_TOPIC_BIN --list`
if [ ! -z "${topics//[[:blank:]]/}" ] ; then
    echo "Warning, this scipt will delete all topics in kafka listed as below:"
    echo "$topics"
    read -r -p "Please comfirm:[y/N] " response
    case $response in
	[yY])
	    for topic in `$KAFKA_TOPIC_BIN --list`; do
		echo "Delete topic $topic ..."
		$KAFKA_TOPIC_BIN --delete --topic $topic
	    done
	    echo "Sleep 5 seconds ..."
	    sleep 5
	    ;;
	*)
	    echo "Will not delete any topics"
	    ;;
    esac
fi

topics=`$KAFKA_TOPIC_BIN --list`
if [ ! -z "${topics//[[:blank:]]/}" ]; then
    echo "Warning, topics remain exist:"
    echo "$topics"
    echo 
    echo "Please wait a few more minutes or clean & restart kafka, zookeeper, then retry $0."
    exit 1
fi

# create internal topics for samza
$KAFKA_TOPIC_BIN --create --topic "$STREAMING_SAMZA_WORDCOUNT_INTERNAL_TOPIC" --partitions ${SAMZA_PARTITIONS} --replication-factor 1
$KAFKA_TOPIC_BIN --create --topic "$STREAMING_SAMZA_STATISTICS_INTERNAL_TOPIC" --partitions 1 --replication-factor 1
$KAFKA_TOPIC_BIN --create --topic "$STREAMING_SAMZA_DISTINCOUNT_INTERNAL_TOPIC" --partitions ${SAMZA_PARTITIONS} --replication-factor 1

# create topic for bench
$KAFKA_TOPIC_BIN --create --topic "$STREAMING_TOPIC_NAME" --partitions ${SAMZA_PARTITIONS} --replication-factor ${SAMZA_REPLICATION_FACTOR}

show_bannar finish