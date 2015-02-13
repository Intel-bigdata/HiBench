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

declare -A HIBENCH_PROP_ENV_MAPPING
declare -A HIBENCH_PROP_ENV_MAPPING_MANDATORY

HIBENCH_PROP_ENV_MAPPING_MANDATORY["HADOOP_HOME"]="hibench.hadoop.home"
HIBENCH_PROP_ENV_MAPPING_MANDATORY["SPARK_HOME"]="hibench.spark.home"
HIBENCH_PROP_ENV_MAPPING_MANDATORY["HDFS_MASTER"]="hibench.hdfs.master"
HIBENCH_PROP_ENV_MAPPING_MANDATORY["REPORT_COLUM_FORMAT"]="hibench.report.formats"
HIBENCH_PROP_ENV_MAPPING_MANDATORY["NUM_MAPS"]="hibench.default.map.parallelism"
HIBENCH_PROP_ENV_MAPPING_MANDATORY["NUM_REDS"]="hibench.default.shuffle.parallelism"
HIBENCH_PROP_ENV_MAPPING["HADOOP_EXECUTABLE"]="hibench.hadoop.executable"
HIBENCH_PROP_ENV_MAPPING["HADOOP_CONF_DIR"]="hibench.hadoop.configure.dir"
HIBENCH_PROP_ENV_MAPPING["HADOOP_EXAMPLES_JAR"]="hibench.hadoop.examples.jar"
HIBENCH_PROP_ENV_MAPPING["HADOOP_VERSION"]="hibench.hadoop.version"
HIBENCH_PROP_ENV_MAPPING["HADOOP_RELEASE"]="hibench.hadoop.release"
HIBENCH_PROP_ENV_MAPPING["SPARK_EXAMPLES_JAR"]="hibench.spark.examples.jar"
HIBENCH_PROP_ENV_MAPPING["HIBENCH_HOME"]="hibench.home"
HIBENCH_PROP_ENV_MAPPING["HIBENCH_CONF"]="hibench.configure.dir"
HIBENCH_PROP_ENV_MAPPING["HIVE_HOME"]="hibench.hive.home"
HIBENCH_PROP_ENV_MAPPING["MAHOUT_HOME"]="hibench.mahout.home"
HIBENCH_PROP_ENV_MAPPING["NUTCH_HOME"]="hibench.nutch.home"
HIBENCH_PROP_ENV_MAPPING["SPARKBENCH_JAR"]="hibench.sparkbench.jar"
HIBENCH_PROP_ENV_MAPPING["HIBENCH_REPORT"]="hibench.report.dir"
HIBENCH_PROP_ENV_MAPPING["YARN_NUM_EXECUTORS"]="hibench.yarn.exectors.num"
HIBENCH_PROP_ENV_MAPPING["YARN_EXECUTOR_CORES"]="hibench.yarn.exectors.cores"
HIBENCH_PROP_ENV_MAPPING["YARN_EXECUTOR_MEMORY"]="hibench.yarn.exectors.memory"
HIBENCH_PROP_ENV_MAPPING["DATA_HDFS"]="hibench.hdfs.data.dir"
HIBENCH_PROP_ENV_MAPPING["MAP_SLEEP_TIME"]="sparkbench.sleep.mapper.seconds"
HIBENCH_PROP_ENV_MAPPING["RED_SLEEP_TIME"]="sparkbench.sleep.reducer.seconds"

# generate reverse mapping
declare -A HIBENCH_PROP_ENV_MAPPING_REVERSE
declare -A HIBENCH_PROP_ENV_MAPPING_MANDATORY_REVERSE

for key in ${!HIBENCH_PROP_ENV_MAPPING[@]}; do
    value=${HIBENCH_PROP_ENV_MAPPING["$key"]}
    HIBENCH_PROP_ENV_MAPPING_REVERSE["$value"]="$key"
done
for key in ${!HIBENCH_PROP_ENV_MAPPING_MANDATORY[@]}; do
    value=${HIBENCH_PROP_ENV_MAPPING_MANDATORY["$key"]}
    HIBENCH_PROP_ENV_MAPPING_MANDATORY_REVERSE["$value"]="$key"
done


