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
. ${config_parser_bin}/hibench-prop-env-mapping.sh

set -u

export HIBENCH_CONF_FOLDER=${config_parser_bin}/../../conf

# store configurations for hibench
declare -A HIBENCH_CONF_CONTAINER 
# store where the configurations comes
declare -A HIBENCH_CONF_REF_CONTAINER

function abspath(){
    assert "$1" "unspecfied path"
    readlink -m $1		# FIXME, will add dependency to "readlink"
}

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
    assert $1 "unspecfied conf root path"
    assert $2 "unspecfied workload root path"
    assert $3 "unspecfied workload folder path"
    local conf_path=$(abspath "$1")
    local workload_root=$(abspath "$2")
    local workload_folder=$(abspath "$3")

    local workload_tail=`dirname ${workload_folder:${#workload_root}:${#workload_folder}}`
    workload_tail=${workload_tail:1}
    local conf_globals=${conf_path}/*.conf
    local conf_workload=${conf_path}/workloads/`basename ${workload_root}`/*.conf
    local conf_workload_api=${conf_path}/workloads/`basename ${workload_root}`/$workload_tail/*.conf
    
    local old_IFS=$IFS
    for file in ${conf_globals} ${conf_workload} ${conf_workload_api}; do
	echo "Parsing conf: $file"
	IFS=$'\n'
	for line in `cat ${file} | grep -v -e '^[[:space:]]*$' | sed "/^#/ d" | sed -re 's/^([a-zA-Z\.]*)\s+(.*)/\1|\2/g'`;  do
	    local key=`echo $line | cut -d '|' -f 1`
	    local value=`echo $line | cut -d '|' -f 2`
	    HIBENCH_CONF_CONTAINER[$key]=$value
	    HIBENCH_CONF_REF_CONTAINER[$key]=$file
#	echo "$key -> ${HIBENCH_CONF_CONTAINER[$key]}  @ ${HIBENCH_CONF_REF_CONTAINER[$key]}"
	done
    done
    IFS=$old_IFS
    mapping_config
    waterfall_config
    generate_value_for_optional_config
    waterfall_config
}

function mapping_config(){  	# mapping properties to environment
    for key in ${!HIBENCH_PROP_ENV_MAPPING_MANDATORY[@]}; do
	value=${HIBENCH_PROP_ENV_MAPPING_MANDATORY["$key"]}
	prop=$(get_prop $value)
	if [[ ${prop} == *\$\{* ]]; then
	    prop=`echo $prop | sed -re 's/[$][{]/\\\\${/g'`
	fi
	eval "${key}=${prop}"
    done
    for key in ${!HIBENCH_PROP_ENV_MAPPING[@]}; do
	value=${HIBENCH_PROP_ENV_MAPPING["$key"]}
	prop=$(get_prop_weak $value)
	if [[ ${prop} == *\$\{* ]]; then
	    prop=`echo $prop | sed -re 's/[$][{]/\\\\${/g'`
	fi
	eval "${key}=${prop}"
    done
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

function waterfall_config(){
    while true; do
	for key in ${!HIBENCH_PROP_ENV_MAPPING[@]}; do
	    value=${!key}
	    if [[ $value == *\$\{* ]]; then
		echo $value
		value=`echo $value | sed -re 's/[$][{][}]`
		eval "$value"
		echo $value
	    fi
	done
	break
    done
}

function generate_sparkbench_prop_files() {
    :    
}

function generate_spark_prop_files() {
    :   
}