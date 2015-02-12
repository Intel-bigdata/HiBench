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

set -u

this="${BASH_SOURCE-$0}"
workload_func_bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
. ${workload_func_bin}/assert.sh
. ${workload_func_bin}/config-parser.sh

function enter_bench(){		# declare the entrance of a workload
    assert $1 "Workload name not specified."
    assert $2 "Workload root not specified."
    assert $3 "Workload folder not specified."
    export HIBENCH_CUR_WORKLOAD_NAME=$1
    load_config ${HIBENCH_CONF_FOLDER} $2 $3
}

function leave_bench(){		# declare the workload is finished
    assert $HIBENCH_CUR_WORKLOAD_NAME "BUG, HIBENCH_CUR_WORKLOAD_NAME unset."
    unset HIBENCH_CUR_WORKLOAD_NAME
}

function show_bannar(){		# print bannar
    assert $HIBENCH_CUR_WORKLOAD_NAME "HIBENCH_CUR_WORKLOAD_NAME not specified."
    assert $1 "Unknown banner operation"
    echo "========== $1 $HIBENCH_CUR_WORKLOAD_NAME bench =========="
}

function timestamp(){		# get current timestamp
    sec=`date +%s`
    nanosec=`date +%N`
    tmp=`expr $sec \* 1000 `
    msec=`expr $nanosec / 1000000 `
    echo `expr $tmp + $msec`
}

function print_field_name() {	# print report column header
    printf "${REPORT_COLUMN_FORMATS}" Type Date Time Input_data_size "Duration(s)" "Throughput(bytes/s)" Throughput/node > ${HIBENCH_REPORT}
}

function gen_report() {		# dump the result to report file
    assert ${HIBENCH_CUR_WORKLOAD_NAME} "HIBENCH_CUR_WORKLOAD_NAME not specified."
    local start=$1
    local end=$2
    local size=$3
    which bc > /dev/null 2>&1
    if [ $? -eq 1 ]; then
        echo "\"bc\" utility missing. Please install it to generate proper report."
        return 1
    fi
    local duration=$(echo "scale=3;($end-$start)/1000"|bc)
    local tput=`echo "$size/$duration"|bc`
    local nodes=`cat ${SPARK_HOME}/conf/slaves 2>/dev/null | grep -v '^\s*$' | sed "/^#/ d" | wc -l`
    nodes=${nodes:-1}
    if [ $nodes -eq 0 ]; then nodes=1; fi
    local tput_node=`echo "$tput/$nodes"|bc`

    if [ ! -f ${HIBENCH_REPORT} ] ; then
        print_field_name
    fi

    printf "${REPORT_COLUMN_FORMATS}" ${HIBENCH_CUR_WORKLOAD_NAME} $(date +%F) $(date +%T) $size $duration $tput $tput_node >> ${HIBENCH_REPORT}
}

function rmr_hdfs(){		# rm -r for hdfs
    if [ "x"$HADOOP_VERSION == "xhadoop2" ]; then
	RMDIR_CMD="fs -rm -r -skipTrash"
    else
	RMDIR_CMD="fs -rmr -skipTrash"
    fi
}

function dus_hdfs(){		# du -s for hdfs
    if [ "x"$HADOOP_VERSION == "xhadoop2" ]; then
	DUS_CMD="fs -du -s"
    else
	DUS_CMD="fs -dus"
    fi 
}


function check_dir() {		# ensure dir is created
    local dir=$1
    assert $1 "dir parameter missing"
    if [ -z "$dir" ];then
        echo "WARN: payload missing."
        return 1
    fi
    if [ ! -d "$dir" ];then
        echo "ERROR: directory $dir does not exist."
        exit 1
    fi
    touch "$dir"/touchtest
    if [ $? -ne 0 ]; then
	echo "ERROR: directory unwritable."
	exit 1
    else
	rm "$dir"/touchtest
    fi
}

function dir_size() {		
    for item in $($HADOOP_EXECUTABLE fs -dus $1); do
        if [[ $item =~ ^[0-9]+$ ]]; then
            echo $item
        fi
    done
}

function check_compress() {
  if [ "x"$HADOOP_VERSION == "xhadoop2" ]; then

     if [ $COMPRESS -eq 1 ]; then
       COMPRESS_OPT="-Dmapreduce.map.output.compress=true \
                     -Dmapreduce.map.output.compress.codec=$COMPRESS_CODEC_MAP \
                     -Dmapreduce.output.fileoutputformat.compress=true \
                     -Dmapreduce.output.fileoutputformat.compress.codec=$COMPRESS_CODEC \
                     -Dmapreduce.output.fileoutputformat.compress.type=BLOCK "
     else
       COMPRESS_OPT="-Dmapreduce.output.fileoutputformat.compress=false "
     fi

  else
    if [ $COMPRESS -eq 1 ]; then

      COMPRESS_OPT="-Dmapred.map.output.compress=true \
                    -Dmapred.map.output.compress.codec=$COMPRESS_CODEC_MAP \
                    -Dmapred.output.compress=true \
                    -Dmapred.output.compression.codec=$COMPRESS_CODEC \
                    -Dmapred.output.compression.type=BLOCK "

    else
      COMPRESS_OPT="-Dmapred.output.compress=false"
    fi
  fi
}

function run-spark-job() {
    LIB_JARS=
    while (($#)); do
      if [ "$1" = "--jars" ]; then
        LIB_JARS="--jars $2"
        shift 2
        continue
      fi
      break
    done

    CLS=$1
    shift
    
    cat ${WORKLOAD_DIR}/../conf/global_properties.conf ${WORKLOAD_DIR}/conf/properties.conf > ${WORKLOAD_DIR}/conf/._prop.conf
    PROP_FILES="${WORKLOAD_DIR}/conf/._prop.conf"
    export SPARKBENCH_PROPERTIES_FILES=${PROP_FILES}

    YARN_OPTS=""
    if [[ "$SPARK_MASTER" == yarn-* ]]; then
       YARN_OPTS="--num-executors ${YARN_NUM_EXECTORS:-1}"
       if [[ -n "${YARN_EXECUTOR_CORES:-}" ]]; then
	   YARN_OPTS="${YARN_OPTS} --executor-cores ${YARN_EXECUTOR_CORES}"
       fi
       if [[ -n "${YARN_EXECUTOR_MEMORY:-}" ]]; then
	   YARN_OPTS="${YARN_OPTS} --executor-memory ${YARN_EXECUTOR_MEMORY}"
       fi
    fi
    if [[ "$CLS" == *.py ]]; then 
	LIB_JARS="$LIB_JARS --jars ${SPARKBENCH_JAR}"
	${SPARK_HOME}/bin/spark-submit ${LIB_JARS} --properties-file ${PROP_FILES} --master ${SPARK_MASTER} ${YARN_OPTS} ${CLS} $@
    else
	echo ${SPARK_HOME}/bin/spark-submit ${LIB_JARS} --properties-file ${PROP_FILES} --class ${CLS} --master ${SPARK_MASTER} ${YARN_OPTS} ${SPARKBENCH_JAR} $@
	${SPARK_HOME}/bin/spark-submit ${LIB_JARS} --properties-file ${PROP_FILES} --class ${CLS} --master ${SPARK_MASTER} ${YARN_OPTS} ${SPARKBENCH_JAR} $@
    fi
    result=$?
    rm -rf ${WORKLOAD_DIR}/conf/._prop.conf 2> /dev/null || true
    if [ $result -ne 0 ]
    then
	echo "ERROR: Spark job ${CLS} failed to run successfully."
	exit $result
    fi
}

function run-hadoop-job(){
    local job_jar=$1
    shift
    local job_name=$1
    shift
    local tail_arguments=$@
    local CMD="${HADOOP_EXECUTABLE} jar $job_jar $job_name $tail_arguments"
    echo "$CMD"
    $CMD
}
