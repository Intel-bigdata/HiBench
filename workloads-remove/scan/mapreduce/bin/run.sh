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

enter_bench HadoopScan ${workload_root} ${workload_folder}
show_bannar start

ensure-hivebench-release

cp ${HIVEBENCH_TEMPLATE}/bin/hive $HIVE_HOME/bin

# path check
rmr-hdfs $OUTPUT_HDFS

# prepare SQL
HIVEBENCH_SQL_FILE=${WORKLOAD_RESULT_FOLDER}/rankings_uservisits_scan.hive
prepare-sql-scan ${HIVEBENCH_SQL_FILE}

# run bench
CMD="$HIVE_HOME/bin/hive -f ${HIVEBENCH_SQL_FILE}"
MONITOR_PID=`start-monitor`
START_TIME=`timestamp`
execute_withlog $CMD
END_TIME=`timestamp`
stop-monitor $MONITOR_PID

sleep 5
SIZE=`dir_size $OUTPUT_HDFS`

gen_report ${START_TIME} ${END_TIME} ${SIZE:-0}
show_bannar finish
leave_bench

