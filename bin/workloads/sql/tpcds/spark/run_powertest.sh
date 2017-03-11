#!/usr/bin/env bash
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

INCLUDED_LIST=(19 42 43 52 55 63 68 73 98)

SET_REDUCE_NUM=()
SET_REDUCE_NUM[19]=1000
SET_REDUCE_NUM[42]=200
SET_REDUCE_NUM[43]=200
SET_REDUCE_NUM[52]=200
SET_REDUCE_NUM[55]=200
SET_REDUCE_NUM[63]=200
SET_REDUCE_NUM[68]=200
SET_REDUCE_NUM[73]=200
SET_REDUCE_NUM[98]=200

SPARK_SQL_CMD="spark-sql"
SPARK_SQL_GLOBAL_OPTS="--driver-memory 1G --hiveconf hive.metastore.uris=thrift://localhost:9083 --executor-memory 11G --executor-cores 8 --conf spark.yarn.executor.memoryOverhead=5120 --conf spark.sql.autoBroadcastJoinThreshold=31457280"
DATABASE_NAME="tpcds_1g"
QUERY_BEGIN_NUM=19
QUERY_END_NUM=100
TUNNING_NAME=256G_mapjoin_32exec_5cores_dfs256m_filesize1g

current_dir=`dirname "$0"`
current_dir=`cd "$current_dir"; pwd`
root_dir=${current_dir}/../../../../../
workload_config=${root_dir}/conf/workloads/sql/tpcds.conf
. "${root_dir}/bin/functions/load-bench-config.sh"

enter_bench SparkPowerTestTpcDS ${workload_config} ${current_dir}
show_bannar start

START_TIME=`timestamp`

runPowerTest
END_TIME=`timestamp`

gen_report ${START_TIME} ${END_TIME} ${SIZE:-0}
show_bannar finish
leave_bench



