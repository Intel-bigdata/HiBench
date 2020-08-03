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
workload_config=${root_dir}/conf/workloads/sql/aggregation.conf
. "${root_dir}/bin/functions/load_bench_config.sh"

enter_bench HadoopAggregation ${workload_config} ${current_dir}
show_bannar start

ensure_hivebench_release


# path check
rmr_hdfs $OUTPUT_HDFS

# prepare SQL
HIVEBENCH_SQL_FILE=${WORKLOAD_RESULT_FOLDER}/uservisits_aggre.hive
prepare_sql_aggregation ${HIVEBENCH_SQL_FILE}


if [[ $HADOOP_HOME =~ "3.2" || $HADOOP_HOME =~ "3.1" ]];then
    echo " replace guava jar nad create metadata schema"
    # replace guava jar
    rm -rf $HIVE_HOME/lib/guava-19.0.jar
    cp ${HIVEBENCH_TEMPLATE}/lib/guava-27.0.1-jre.jar $HIVE_HOME/lib
    # create metadata schema
    rm -rf $HIVE_HOME/../metastore_db
    echo "$HIVE_HOME/bin/schematool -initSchema -dbType derby"
    $HIVE_HOME/bin/schematool -initSchema -dbType derby
elif [[ $HADOOP_HOME =~ "3.0" ]];then
    echo " create metadata schema"
    # create metadata schema
    rm -rf $HIVE_HOME/../metastore_db
    echo "$HIVE_HOME/bin/schematool -initSchema -dbType derby"
    $HIVE_HOME/bin/schematool -initSchema -dbType derby
fi


# run bench
MONITOR_PID=`start_monitor`
START_TIME=`timestamp`
CMD="$HIVE_HOME/bin/hive -f ${HIVEBENCH_SQL_FILE}"
echo $CMD
execute_withlog $CMD
END_TIME=`timestamp`
stop_monitor $MONITOR_PID

sleep 5
SIZE=`dir_size $OUTPUT_HDFS`

gen_report ${START_TIME} ${END_TIME} ${SIZE:-}

show_bannar finish
leave_bench
