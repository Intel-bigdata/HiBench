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
workload_config=${root_dir}/conf/workloads/graph/nweight.conf
. "${root_dir}/bin/functions/load_bench_config.sh"

enter_bench NWeightPrepare ${workload_config} ${current_dir}
show_bannar start

rmr_hdfs $INPUT_HDFS || true
START_TIME=`timestamp`

run_spark_job com.intel.hibench.sparkbench.graph.nweight.NWeightDataGenerator $MODEL_INPUT $INPUT_HDFS $EDGES

END_TIME=`timestamp`

show_bannar finish
leave_bench

