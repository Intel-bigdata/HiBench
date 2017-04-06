#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE_2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

current_dir=`dirname "$0"`
current_dir=`cd "$current_dir"; pwd`
root_dir=${current_dir}/../../../../../
workload_config=${root_dir}/conf/workloads/sql/tpcds.conf
. "${root_dir}/bin/functions/load_bench_config.sh"

enter_bench SparkPrepareTpcDS ${workload_config} ${current_dir}
show_bannar start

ensure_tpcds_kit_ready
rmr_hdfs $INPUT_HDFS || true
upload_to_hdfs ${DSDGEN_DIR} ${INPUT_HDFS}/tools
DSDGEN_DIR=${INPUT_HDFS}/tools

echo -e "${On_Blue}Prepare Tpc-DS: working${Color_Off}"

START_TIME=`timestamp`
run_spark_job com.intel.hibench.sparkbench.sql.tpcds.DataGen ${INPUT_HDFS} ${TABLE_SIZE} ${DSDGEN_DIR} ${HADOOP_HOME}
END_TIME=`timestamp`

show_bannar finish
leave_bench
