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

this="${BASH_SOURCE-$0}"
config_parser_bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
. ${config_parser_bin}/assert.sh

set -u

export HIBENCH_CONF_FOLDER=${config_parser_bin}/../../conf
declare -A HIBENCH_CONF_CONTAINER

function get_prop(){
    assert "$1" "unspecfied key in get_prop"

#    echo ${HIBENCH_CONF_CONTAINER[@]} > /dev/stderr
    local val=${HIBENCH_CONF_CONTAINER["$1"]:-}
    assert "$val" "Undefined value $1"
#    echo $val > /dev/stderr
    echo $val
}

function get_prop_weak(){
    assert $1 "unspecfied key in get_prop"

    echo ${HIBENCH_CONF_CONTAINER["$1"]:-}
}

function load_config(){
    assert $1 "unspecfied conf folder path"
    assert $2 "unspecfied workload folder path"
    local conf_path=$1
    local workload_conf_path=$2/../../conf
    
    local old_IFS=$IFS
    IFS=$'\n'
    for line in `cat ${conf_path} ${workload_conf_path}/*.conf | grep -v -e '^[[:space:]]*$' | sed "/^#/ d" | sed -re 's/^([a-zA-Z\.]*)\s+(.*)/\1|\2/g'`;  do
	local key=`echo $line | cut -d '|' -f 1`
	local value=`echo $line | cut -d '|' -f 2`
	HIBENCH_CONF_CONTAINER[$key]=$value
	echo "$key -> ${HIBENCH_CONF_CONTAINER[$key]} "
    done
    IFS=$old_IFS
    mapping_config
    generate_value_for_optional_config
}

function mapping_config(){  	# mapping properties to environment
    HADOOP_HOME=$(get_prop "hibench.hadoop.home")
    SPARK_HOME=$(get_prop "hibench.spark.home")
    HDFS_MASTER=$(get_prop "hibench.hdfs.master")

    HADOOP_EXECUTABLE=$(get_prop_weak "hibench.hadoop.executable")
    HADOOP_CONF_DIR=$(get_prop_weak "hibench.hadoop.configure.dir")
    HADOOP_EXAMPLES_JAR=$(get_prop_weak "hibench.hadoop.examples.jar")
    HADOOP_VERSION=$(get_prop_weak "hibench.hadoop.version")
    HADOOP_RELEASE=$(get_prop_weak "hibench.hadoop.release")
    
    SPARK_EXAMPLES_JAR=$(get_prop_weak "hibench.spark.examples.jar")
    HIBENCH_CONF=$(get_prop_weak "hibench.configure.dir")
    HIVE_HOME=$(get_prop_weak "hibench.hive.home")
    MAHOUT_HOME=$(get_prop_weak "hibench.mahout.home")
    NUTCH_HOME=$(get_prop_weak "hibench.nutch.home")
    
    SPARKBENCH_JAR=$(get_prop_weak "hibench.sparkbench.jar")
    HIBENCH_REPORT=$(get_prop_weak "hibench.report.dir")
    REPORT_COLUMN_FORMATS=$(get_prop "hibench.report.formats")

    YARN_NUM_EXECUTORS=$(get_prop_weak "hibench.yarn.exectors.num")
    YARN_EXECUTOR_CORES=$(get_prop_weak "hibench.yarn.exectors.cores")
    YARN_EXECUTOR_MEMORY=$(get_prop_weak "hibench.yarn.exectors.memory")

    DATA_HDFS=$(get_prop_weak "hibench.hdfs.data.dir")
    NUM_MAPS=$(get_prop "hibench.default.map.parallelism")
    NUM_REDS=$(get_prop "hibench.default.shuffle.parallelism")

    #workload specified mapping
    #sleep
    MAP_SLEEP_TIME=$(get_prop_weak "sparkbench.sleep.mapper.seconds")
    RED_SLEEP_TIME=$(get_prop_weak "sparkbench.sleep.reducer.seconds")
}

function generate_value_for_optional_config(){
    if [ -z ${HADOOP_EXECUTABLE} ]; then HADOOP_EXECUTABLE=${HADOOP_HOME}/bin/hadoop; fi
    if [ -z ${HADOOP_VERSION} ]; then 
	local hadoop_version_full=`${HADOOP_EXECUTABLE} version | head -1 | cut -d \   -f 2`; 
	HADOOP_VERSION="hadoop${hadoop_version_full:0:1}"
    fi
    if [ -z ${HADOOP_RELEASE} ]; then 
	local hadoop_version_full=`${HADOOP_EXECUTABLE} version | head -1 | cut -d \   -f 2`; 
	if [ `echo $hadoop_version_full | grep cdh4` ]; then
	    HADOOP_RELEASE=cdh4
	elif [ `echo $hadoop_version_full | grep cdh5` ]; then
	    HADOOP_RELEASE=cdh5
	else
	    HADOOP_RELEASE=HADOOP_VERSION
	fi
    fi
    if [ -z ${HADOOP_EXAMPLES_JAR} ]; then 
	if [ ${HADOOP_RELEASE} == "hadoop1" ]; then
	    HADOOP_EXAMPLES_JAR=${HADOOP_HOME}/hadoop-examples*.jar; 
	else
	    HADOOP_EXAMPLES_JAR=${HADOOP_HOME}/hadoop-examples*.jar; # FIXME: should be $HADOOP_JOBCLIENT_TESTS_JAR
	fi
    fi
}