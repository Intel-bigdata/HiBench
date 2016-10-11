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

export HIBENCH_PRINTFULLLOG=0
this="${BASH_SOURCE-$0}"
workload_func_bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
. ${workload_func_bin}/assert.sh
. ${workload_func_bin}/color.sh

HIBENCH_CONF_FOLDER=${HIBENCH_CONF_FOLDER:-${workload_func_bin}/../../conf}

function enter_bench(){		# declare the entrance of a workload
    assert $1 "Workload name not specified."
    assert $2 "Workload config file not specified."
    export HIBENCH_CUR_WORKLOAD_NAME=$1
    workload_config_file=$2
    shift 2
    patching_args=$@
    echo "patching args=$patching_args"
    local CONF_FILE=`${workload_func_bin}/load_config.py ${HIBENCH_CONF_FOLDER}  $workload_config_file $HIBENCH_CUR_WORKLOAD_NAME $patching_args`
    . $CONF_FILE
}

function leave_bench(){		# declare the workload is finished
    assert $HIBENCH_CUR_WORKLOAD_NAME "BUG, HIBENCH_CUR_WORKLOAD_NAME unset."
    unset HIBENCH_CUR_WORKLOAD_NAME
}

function show_bannar(){		# print bannar
    assert $HIBENCH_CUR_WORKLOAD_NAME "HIBENCH_CUR_WORKLOAD_NAME not specified."
    assert $1 "Unknown banner operation"
    echo -e "${BGreen}$1 ${Color_Off}${UGreen}$HIBENCH_CUR_WORKLOAD_NAME${Color_Off} ${BGreen}bench${Color_Off}"
}

function timestamp(){		# get current timestamp
    sec=`date +%s`
    nanosec=`date +%N`
    re='^[0-9]+$'
    if ! [[ $nanosec =~ $re ]] ; then
	$nanosec=0
    fi
    tmp=`expr $sec \* 1000 `
    msec=`expr $nanosec / 1000000 `
    echo `expr $tmp + $msec`
}

function start-monitor(){
    MONITOR_PID=`${workload_func_bin}/monitor.py ${HIBENCH_CUR_WORKLOAD_NAME} $$ ${WORKLOAD_RESULT_FOLDER}/monitor.log ${WORKLOAD_RESULT_FOLDER}/bench.log ${WORKLOAD_RESULT_FOLDER}/monitor.html ${SLAVES} &`
#    echo "start monitor, got child pid:${MONITOR_PID}" > /dev/stderr
    echo ${MONITOR_PID}
}

function stop-monitor(){
    MONITOR_PID=$1
    assert $1 "monitor pid missing"
#    echo "stop monitor, kill ${MONITOR_PID}" > /dev/stderr
    kill ${MONITOR_PID}
}

function get_field_name() {	# print report column header
    printf "${REPORT_COLUMN_FORMATS}" Type Date Time Input_data_size "Duration(s)" "Throughput(bytes/s)" Throughput/node 
}

function gen_report() {		# dump the result to report file
    assert ${HIBENCH_CUR_WORKLOAD_NAME} "HIBENCH_CUR_WORKLOAD_NAME not specified."
    local start=$1
    local end=$2
    local size=$3
    which bc > /dev/null 2>&1
    if [ $? -eq 1 ]; then
	assert 0 "\"bc\" utility missing. Please install it to generate proper report."
        return 1
    fi
    local duration=$(echo "scale=3;($end-$start)/1000"|bc)
    local tput=`echo "$size/$duration"|bc`
#    local nodes=`cat ${SPARK_HOME}/conf/slaves 2>/dev/null | grep -v '^\s*$' | sed "/^#/ d" | wc -l`
    local nodes=`echo ${SLAVES} | wc -w`
    nodes=${nodes:-1}
    
    if [ $nodes -eq 0 ]; then nodes=1; fi
    local tput_node=`echo "$tput/$nodes"|bc`

    REPORT_TITLE=`get_field_name`
    if [ ! -f ${HIBENCH_REPORT}/${HIBENCH_REPORT_NAME} ] ; then
        echo "${REPORT_TITLE}" > ${HIBENCH_REPORT}/${HIBENCH_REPORT_NAME}
    fi

    REPORT_LINE=$(printf "${REPORT_COLUMN_FORMATS}" ${HIBENCH_CUR_WORKLOAD_NAME} $(date +%F) $(date +%T) $size $duration $tput $tput_node)
    echo "${REPORT_LINE}" >> ${HIBENCH_REPORT}/${HIBENCH_REPORT_NAME}
    echo "# ${REPORT_TITLE}" >> ${HIBENCH_WORKLOAD_CONF}
    echo "# ${REPORT_LINE}" >> ${HIBENCH_WORKLOAD_CONF}
}

function rmr-hdfs(){		# rm -r for hdfs
    assert $1 "dir parameter missing"
    if [ $HADOOP_VERSION == "hadoop1" ] && [ "$HADOOP_RELEASE" == "apache" ]; then
        RMDIR_CMD="fs -rmr -skipTrash"
    else
        RMDIR_CMD="fs -rm -r -skipTrash"
    fi
    local CMD="$HADOOP_EXECUTABLE --config $HADOOP_CONF_DIR $RMDIR_CMD $1"
    echo -e "${BCyan}hdfs rm -r: ${Cyan}${CMD}${Color_Off}" > /dev/stderr
    execute_withlog ${CMD}
}

function upload-to-hdfs(){
    assert $1 "local parameter missing"
    assert $2 "remote parameter missing"
    LOCAL_FILE_PATH=$1
    REMOTE_FILE_PATH=$2
    echo "REMOTE_FILE_PATH:$REMOTE_FILE_PATH" > /dev/stderr
    if [[ `echo $REMOTE_FILE_PATH | tr A-Z a-z` = hdfs://* ]]; then # strip leading "HDFS://xxx:xxx/" string
        echo "HDFS_MASTER:$HDFS_MASTER" > /dev/stderr
        local LEADING_HDFS_STRING_LENGTH=${#HDFS_MASTER}
        REMOTE_FILE_PATH=${REMOTE_FILE_PATH:$LEADING_HDFS_STRING_LENGTH}
        echo "stripped REMOTE_FILE_PATH:$REMOTE_FILE_PATH" > /dev/stderr
    fi

    # clear previous package file
    local CMD="$HADOOP_EXECUTABLE --config $HADOOP_CONF_DIR fs -rm $REMOTE_FILE_PATH"
    echo -e "${BCyan}hdfs rm : ${Cyan}${CMD}${Color_Off}" > /dev/stderr
    execute_withlog ${CMD}

    # prepare parent folder
    CMD="$HADOOP_EXECUTABLE --config $HADOOP_CONF_DIR fs -mkdir `dirname $REMOTE_FILE_PATH`"
    echo -e "${BCyan}hdfs mkdir : ${Cyan}${CMD}${Color_Off}" > /dev/stderr
    execute_withlog ${CMD}

    # upload
    CMD="$HADOOP_EXECUTABLE --config $HADOOP_CONF_DIR fs -put $LOCAL_FILE_PATH $REMOTE_FILE_PATH"
    echo -e "${BCyan}hdfs put : ${Cyan}${CMD}${Color_Off}" > /dev/stderr
    execute_withlog ${CMD}
}

function dus-hdfs(){                # du -s for hdfs
    assert $1 "dir parameter missing"
    if [ $HADOOP_VERSION == "hadoop1" ] && [ "$HADOOP_RELEASE" == "apache" ]; then
        DUS_CMD="fs -dus"
    else
        DUS_CMD="fs -du -s"
    fi
    local CMD="$HADOOP_EXECUTABLE --config $HADOOP_CONF_DIR $DUS_CMD $1"
    echo -e "${BPurple}hdfs du -s: ${Purple}${CMD}${Color_Off}" > /dev/stderr
    execute_withlog ${CMD}
}


function check_dir() {                # ensure dir is created
    local dir=$1
    assert $1 "dir parameter missing"
    if [ -z "$dir" ];then
        echo -e "${BYellow}WARN${Color_Off}: payload missing."
        return 1
    fi
    if [ ! -d "$dir" ];then
        echo -e "${BRed}ERROR${Color_Off}: directory $dir does not exist."
        exit 1
    fi
    touch "$dir"/touchtest
    if [ $? -ne 0 ]; then
        echo -e "${BRed}ERROR${Color_Off}: directory unwritable."
        exit 1
    else
        rm "$dir"/touchtest
    fi
}

function dir_size() {                
    for item in $(dus-hdfs $1); do
        if [[ $item =~ ^[0-9]+$ ]]; then
            echo $item
        fi
    done
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

    export_withlog SPARKBENCH_PROPERTIES_FILES

    YARN_OPTS=""
    if [[ "$SPARK_MASTER" == yarn-* ]]; then
        export_withlog HADOOP_CONF_DIR
        
        YARN_OPTS="--num-executors ${YARN_NUM_EXECUTORS}"
        if [[ -n "${YARN_EXECUTOR_CORES:-}" ]]; then
            YARN_OPTS="${YARN_OPTS} --executor-cores ${YARN_EXECUTOR_CORES}"
       fi
       if [[ -n "${SPARK_YARN_EXECUTOR_MEMORY:-}" ]]; then
           YARN_OPTS="${YARN_OPTS} --executor-memory ${SPARK_YARN_EXECUTOR_MEMORY}"
       fi
       if [[ -n "${SPAKR_YARN_DRIVER_MEMORY:-}" ]]; then
           YARN_OPTS="${YARN_OPTS} --driver-memory ${SPARK_YARN_DRIVER_MEMORY}"
       fi
    fi
    if [[ "$CLS" == *.py ]]; then 
        LIB_JARS="$LIB_JARS --jars ${SPARKBENCH_JAR}"
        SUBMIT_CMD="${SPARK_HOME}/bin/spark-submit ${LIB_JARS} --properties-file ${SPARK_PROP_CONF} --master ${SPARK_MASTER} ${YARN_OPTS} ${CLS} $@"
    else
        SUBMIT_CMD="${SPARK_HOME}/bin/spark-submit ${LIB_JARS} --properties-file ${SPARK_PROP_CONF} --class ${CLS} --master ${SPARK_MASTER} ${YARN_OPTS} ${SPARKBENCH_JAR} $@"
    fi
    echo -e "${BGreen}Submit Spark job: ${Green}${SUBMIT_CMD}${Color_Off}"
    MONITOR_PID=`start-monitor`
    execute_withlog ${SUBMIT_CMD}
    result=$?
    stop-monitor ${MONITOR_PID}
    if [ $result -ne 0 ]
    then
        echo -e "${BRed}ERROR${Color_Off}: Spark job ${BYellow}${CLS}${Color_Off} failed to run successfully."
        echo -e "${BBlue}Hint${Color_Off}: You can goto ${BYellow}${WORKLOAD_RESULT_FOLDER}/bench.log${Color_Off} to check for detailed log.\nOpening log tail for you:\n"
        tail ${WORKLOAD_RESULT_FOLDER}/bench.log
        exit $result
    fi
}

function run-streaming-job (){
    run-spark-job --jars ${STREAMINGBENCH_JARS} $@
}

function run-storm-job(){
    CMD="${STORM_BIN_HOME}/storm jar ${STREAMBENCH_STORM_JAR} $@"
    echo -e "${BGreen}Submit Storm Job: ${Green}$CMD${Color_Off}"
    execute_withlog $CMD
}

function run-hadoop-job(){
    ENABLE_MONITOR=1
    if [ "$1" = "--without-monitor" ]; then
        ENABLE_MONITOR=0
        shift 1
    fi
    local job_jar=$1
    shift
    local job_name=$1
    shift
    local tail_arguments=$@
    local CMD="${HADOOP_EXECUTABLE} --config ${HADOOP_CONF_DIR} jar $job_jar $job_name $tail_arguments"
    echo -e "${BGreen}Submit MapReduce Job: ${Green}$CMD${Color_Off}"
    if [ ${ENABLE_MONITOR} = 1 ]; then
        MONITOR_PID=`start-monitor`
    fi
    execute_withlog ${CMD}
    result=$?
    if [ ${ENABLE_MONITOR} = 1 ]; then
        stop-monitor ${MONITOR_PID}
    fi
    if [ $result -ne 0 ]; then
        echo -e "${BRed}ERROR${Color_Off}: Hadoop job ${BYellow}${job_jar} ${job_name}${Color_Off} failed to run successfully."
        echo -e "${BBlue}Hint${Color_Off}: You can goto ${BYellow}${WORKLOAD_RESULT_FOLDER}/bench.log${Color_Off} to check for detailed log.\nOpening log tail for you:\n"
        tail ${WORKLOAD_RESULT_FOLDER}/bench.log
        exit $result
    fi
}

function ensure-hivebench-release(){
    if [ ! -e ${DEPENDENCY_DIR}"/hivebench/target/"$HIVE_RELEASE".tar.gz" ]; then
        assert 0 "Error: The hive bin file hasn't be downloaded by maven, please check!"
        exit
    fi

    cd ${DEPENDENCY_DIR}"/hivebench/target"
    if [ ! -d $HIVE_HOME ]; then
        tar zxf $HIVE_RELEASE".tar.gz"
    fi
    export_withlog HADOOP_EXECUTABLE
}

function ensure-mahout-release (){
    if [ ! -e ${HIBENCH_HOME}"/mahout/target/"$MAHOUT_RELEASE".tar.gz" ]; then
        assert 0 "Error: The mahout bin file hasn't be downloaded by maven, please check!"
        exit
    fi

    cd ${HIBENCH_HOME}"/mahout/target"
    if [ ! -d $MAHOUT_HOME ]; then
        tar zxf $MAHOUT_RELEASE".tar.gz"
    fi
    export_withlog HADOOP_EXECUTABLE
    export_withlog HADOOP_HOME
    export_withlog HADOOP_CONF_DIR    
}

function execute () {
    CMD="$@"
    echo -e "${BCyan}Executing: ${Cyan}${CMD}${Color_Off}"
    $CMD
}

function printFullLog(){
    export HIBENCH_PRINTFULLLOG=1
}

function execute_withlog () {
    CMD="$@"
    if [ -t 1 ] ; then          # Terminal, beautify the output.
        ${workload_func_bin}/execute_with_log.py ${WORKLOAD_RESULT_FOLDER}/bench.log $CMD
    else                        # pipe, do nothing.
        $CMD
    fi
}


function export_withlog () {
    var_name=$1
    var_val=${!1}
    assert $1 "export without a variable name!"
    echo -e "${BCyan}Export env: ${Cyan}${var_name}${BCyan}=${Cyan}${var_val}${Color_Off}"
    export ${var_name}
}

function ensure-nutchindexing-release () {
    if [ ! -e ${DEPENDENCY_DIR}"/nutchindexing/target/apache-nutch-1.2-bin.tar.gz" ]; then
        assert 0 "Error: The nutch bin file hasn't be downloaded by maven, please check!"
        exit
    fi

    NUTCH_ROOT=${WORKLOAD_RESULT_FOLDER}
    cp -a $NUTCH_DIR/nutch $NUTCH_ROOT

    if [ $HADOOP_VERSION == "hadoop1" ]; then
        cp $NUTCH_ROOT/nutch/conf/nutch-site-mr1.xml $NUTCH_ROOT/nutch/conf/nutch-site.xml
    elif [ $HADOOP_VERSION == "hadoop2" ]; then
        cp $NUTCH_ROOT/nutch/conf/nutch-site-mr2.xml $NUTCH_ROOT/nutch/conf/nutch-site.xml
    fi
    
    cd ${DEPENDENCY_DIR}"/nutchindexing/target"
    if [ ! -d $NUTCH_HOME ]; then
        tar zxf apache-nutch-1.2-bin.tar.gz
    fi
    find $NUTCH_HOME/lib ! -name "lucene-*" -type f -exec rm -rf {} \;
    rm -rf $NUTCH_ROOT/nutch_release
    cp -a $NUTCH_HOME $NUTCH_ROOT/nutch_release
    NUTCH_HOME_WORKLOAD=$NUTCH_ROOT/nutch_release
    cp $NUTCH_ROOT/nutch/conf/nutch-site.xml $NUTCH_HOME_WORKLOAD/conf
    cp $NUTCH_ROOT/nutch/bin/nutch $NUTCH_HOME_WORKLOAD/bin

    # Patching jcl-over-slf4j version against cdh or hadoop2
    if [ $HADOOP_VERSION == "hadoop2" ] || [ ${HADOOP_RELEASE:0:3} == "cdh" ]; then
        mkdir $NUTCH_HOME_WORKLOAD/temp
        unzip -q $NUTCH_HOME_WORKLOAD/nutch-1.2.job -d $NUTCH_HOME_WORKLOAD/temp
        rm -f $NUTCH_HOME_WORKLOAD/temp/lib/jcl-over-slf4j-*.jar
        rm -f $NUTCH_HOME_WORKLOAD/temp/lib/slf4j-log4j*.jar
        cp ${NUTCH_DIR}/target/dependency/jcl-over-slf4j-*.jar $NUTCH_HOME_WORKLOAD/temp/lib
        rm -f $NUTCH_HOME_WORKLOAD/nutch-1.2.job
        cd $NUTCH_HOME_WORKLOAD/temp
        zip -qr $NUTCH_HOME_WORKLOAD/nutch-1.2.job *
        rm -rf $NUTCH_HOME_WORKLOAD/temp
    fi

    echo $NUTCH_HOME_WORKLOAD
}

function prepare-sql-aggregation () {
    assert $1 "SQL file path not exist"
    HIVEBENCH_SQL_FILE=$1

    find . -name "metastore_db" -exec rm -rf "{}" \; 2>/dev/null

    cat <<EOF > ${HIVEBENCH_SQL_FILE}
USE DEFAULT;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set ${MAP_CONFIG_NAME}=$NUM_MAPS;
set ${REDUCER_CONFIG_NAME}=$NUM_REDS;
set hive.stats.autogather=false;
${HIVE_SQL_COMPRESS_OPTS}

CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS SEQUENCEFILE LOCATION '$INPUT_HDFS/uservisits';
CREATE EXTERNAL TABLE uservisits_aggre ( sourceIP STRING, sumAdRevenue DOUBLE) STORED AS SEQUENCEFILE LOCATION '$OUTPUT_HDFS/uservisits_aggre';
INSERT OVERWRITE TABLE uservisits_aggre SELECT sourceIP, SUM(adRevenue) FROM uservisits GROUP BY sourceIP;
EOF
}

function prepare-sql-join () {
    assert $1 "SQL file path not exist"
    HIVEBENCH_SQL_FILE=$1

    find . -name "metastore_db" -exec rm -rf "{}" \; 2>/dev/null

    cat <<EOF > ${HIVEBENCH_SQL_FILE}
USE DEFAULT;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set ${MAP_CONFIG_NAME}=$NUM_MAPS;
set ${REDUCER_CONFIG_NAME}=$NUM_REDS;
set hive.stats.autogather=false;

${HIVE_SQL_COMPRESS_OPTS}

CREATE EXTERNAL TABLE rankings (pageURL STRING, pageRank INT, avgDuration INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS SEQUENCEFILE LOCATION '$INPUT_HDFS/rankings';
CREATE EXTERNAL TABLE uservisits_copy (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS SEQUENCEFILE LOCATION '$INPUT_HDFS/uservisits';
CREATE EXTERNAL TABLE rankings_uservisits_join ( sourceIP STRING, avgPageRank DOUBLE, totalRevenue DOUBLE) STORED AS SEQUENCEFILE LOCATION '$OUTPUT_HDFS/rankings_uservisits_join';
INSERT OVERWRITE TABLE rankings_uservisits_join SELECT sourceIP, avg(pageRank), sum(adRevenue) as totalRevenue FROM rankings R JOIN (SELECT sourceIP, destURL, adRevenue FROM uservisits_copy UV WHERE (datediff(UV.visitDate, '1999-01-01')>=0 AND datediff(UV.visitDate, '2000-01-01')<=0)) NUV ON (R.pageURL = NUV.destURL) group by sourceIP order by totalRevenue DESC;
EOF
}

function prepare-sql-scan () {
    assert $1 "SQL file path not exist"
    HIVEBENCH_SQL_FILE=$1

    find . -name "metastore_db" -exec rm -rf "{}" \; 2>/dev/null

    cat <<EOF > ${HIVEBENCH_SQL_FILE}
USE DEFAULT;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set ${MAP_CONFIG_NAME}=$NUM_MAPS;
set ${REDUCER_CONFIG_NAME}=$NUM_REDS;
set hive.stats.autogather=false;

${HIVE_SQL_COMPRESS_OPTS}

CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS SEQUENCEFILE LOCATION '$INPUT_HDFS/uservisits';
CREATE EXTERNAL TABLE uservisits_copy (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS SEQUENCEFILE LOCATION '$OUTPUT_HDFS/uservisits_copy';
INSERT OVERWRITE TABLE uservisits_copy SELECT * FROM uservisits;
EOF

}
