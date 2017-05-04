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

current_dir=`dirname "$0"`
current_dir=`cd "$current_dir"; pwd`
root_dir=${current_dir}/../../../../../
workload_config=${root_dir}/conf/workloads/websearch/nutchindexing.conf
. "${root_dir}/bin/functions/load_bench_config.sh"

enter_bench HadoopNutchindexing ${workload_config} ${current_dir}
show_bannar start

rmr_hdfs $OUTPUT_HDFS || true

command_exist zip 
if [ $? -ne 0 ] 
  then
      echo -e "\e[31mcan't find zip command,for ubuntu system, you can try use command line 'apt-get install zip'\e[0m" 
      exit
fi
command_exist unzip
if [ $? -ne 0 ] 
  then
      echo -e "\e[31mcan't find unzip command,for ubuntu system, you can try use command line 'apt-get install unzip'\e[0m" 
      exit
fi


NUTCH_HOME_WORKLOAD=`ensure_nutchindexing_release`
NUTCH_DEPENDENCY_DIR=$root_dir"/hadoopbench/nutchindexing/target/dependency"
cd $NUTCH_HOME_WORKLOAD
SIZE=`dir_size $INPUT_HDFS`
MONITOR_PID=`start_monitor`
START_TIME=`timestamp`

export_withlog HIBENCH_WORKLOAD_CONF
NUTCH_CONF_DIR=$HADOOP_CONF_DIR:$NUTCH_HOME_WORKLOAD/conf
export_withlog NUTCH_CONF_DIR

CMD="$NUTCH_HOME_WORKLOAD/bin/nutch index -D mapreduce.job.reduces=$NUM_REDS -D mapreduce.output.fileoutputformat.compress=false $OUTPUT_HDFS $INPUT_HDFS/crawldb $INPUT_HDFS/linkdb $INPUT_HDFS/segments/*"
echo $CMD
execute_withlog $CMD

END_TIME=`timestamp`
stop_monitor $MONITOR_PID

gen_report ${START_TIME} ${END_TIME} ${SIZE}
show_bannar finish
leave_bench


# pre-running
#SIZE=`dir_size $INPUT_HDFS`
#SIZE=`$HADOOP_EXECUTABLE fs -dus $INPUT_HDFS |  grep -o [0-9]* `
#export NUTCH_CONF_DIR=$HADOOP_CONF_DIR:$NUTCH_HOME/conf

#$NUTCH_HOME/bin/nutch index $COMPRESS_OPTS $OUTPUT_HDFS $INPUT_HDFS/crawldb $INPUT_HDFS/linkdb $INPUT_HDFS/segments/*
