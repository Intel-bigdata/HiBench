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

############### common functions ################
FORMATS="%-12s %-10s %-8s %-20s %-20s %-20s %-20s\n"
function timestamp(){
    sec=`date +%s`
    nanosec=`date +%N`
    tmp=`expr $sec \* 1000 `
    msec=`expr $nanosec / 1000000 `
    echo `expr $tmp + $msec`
}

function print_field_name() {
    printf "$FORMATS" Type Date Time Input_data_size "Duration(s)" "Throughput(bytes/s)" Throughput/node > $SPARKBENCH_REPORT
}

function gen_report() {
    local type=$1
    local start=$2
    local end=$3
    local size=$4
    which bc > /dev/null 2>&1
    if [ $? -eq 1 ]; then
        echo "\"bc\" utility missing. Please install it to generate proper report."
        return 1
    fi
    local duration=$(echo "scale=3;($end-$start)/1000"|bc)
    local tput=`echo "$size/$duration"|bc`
    local nodes=`cat ${SPARK_HOME}/conf/slaves | grep -v '^\s*$' | sed "/^#/ d" | wc -l` 
    local tput_node=`echo "$tput/$nodes"|bc`

    if [ ! -f $SPARKBENCH_REPORT ] ; then
        print_field_name
    fi

    printf "$FORMATS" $type $(date +%F) $(date +%T) $size $duration $tput $tput_node >> $SPARKBENCH_REPORT
}


function check_dir() {
    local dir=$1
    if [ -z "$dir" ];then
        echo "WARN: payload missing."
        return 1
    fi
    if [ ! -d "$dir" ];then
        echo "ERROR: directory $dir does not exist."
        exit 1
    fi
}

function dir_size() {
    for item in $($HADOOP_EXECUTABLE fs -dus $1); do
        if [[ $item =~ ^[0-9]+$ ]]; then
            echo $item
        fi
    done
}

function run-spark-job() {
    CLS=$1
    shift
    
    if [ -d $DIR/prepare ]; then
	WORKLOAD_DIR=$DIR
    elif [ -d $DIR/../prepare ]; then
	WORKLOAD_DIR=$DIR/..
    else
	echo "Unknown workload path!"
	exit 1
    fi
    cat ${WORKLOAD_DIR}/../conf/global_properties.conf ${WORKLOAD_DIR}/conf/properties.conf > ${WORKLOAD_DIR}/conf/._prop.conf
    PROP_FILES="--properties-file ${WORKLOAD_DIR}/conf/._prop.conf"

    if [[ "$CLS" == *.py ]]; then 
	${SPARK_HOME}/bin/spark-submit ${PROP_FILES} --master ${SPARK_MASTER} ${CLS} $@
    else
	${SPARK_HOME}/bin/spark-submit ${PROP_FILES} --class ${CLS} --master ${SPARK_MASTER} ${SPARKBENCH_JAR} $@
    fi
    result=$?
    rm -rf ${WORKLOAD_DIR}/conf/._prop.conf 2> /dev/null || true
    if [ $result -ne 0 ]
    then
	echo "ERROR: Spark job ${CLS} failed to run successfully."
	exit $result
    fi
}
