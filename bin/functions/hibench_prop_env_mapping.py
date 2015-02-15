#!/usr/bin/env python
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

HiBenchEnvPropMappingMandatory=dict(
    HADOOP_HOME="hibench.hadoop.home",
    SPARK_HOME="hibench.spark.home",
    HDFS_MASTER="hibench.hdfs.master",
    SPARK_MASTER="hibench.spark.master",
    REPORT_COLUMN_FORMATS="hibench.report.formats",
    SPARKBENCH_JAR="hibench.sparkbench.jar",
    HIBENCH_PYTHON_PATH="hibench.sparkbenc.python.dir",
    NUM_MAPS="hibench.default.map.parallelism",
    NUM_REDS="hibench.default.shuffle.parallelism",
    )

HiBenchEnvPropMapping=dict(
    HADOOP_EXECUTABLE="hibench.hadoop.executable", # defined
    HADOOP_CONF_DIR="hibench.hadoop.configure.dir", # defined
    HADOOP_VERSION="hibench.hadoop.version",        # inferred
    HADOOP_RELEASE="hibench.hadoop.release",        # inferred
    HADOOP_EXAMPLES_JAR="hibench.hadoop.examples.jar", # inferred
    SPARK_EXAMPLES_JAR="hibench.spark.examples.jar",
    HIBENCH_HOME="hibench.home", # inferred
    HIBENCH_CONF="hibench.configure.dir", # set in default
    HIVE_HOME="hibench.hive.home",
    MAHOUT_HOME="hibench.mahout.home",
    NUTCH_HOME="hibench.nutch.home",
    HIBENCH_REPORT="hibench.report.dir", # set in default
    YARN_NUM_EXECUTORS="hibench.yarn.exectors.num",
    YARN_EXECUTOR_CORES="hibench.yarn.exectors.cores",
    YARN_EXECUTOR_MEMORY="hibench.yarn.exectors.memory",
    DATA_HDFS="hibench.hdfs.data.dir",
    MAP_SLEEP_TIME="sparkbench.sleep.mapper.seconds", # set in sleep workload
    RED_SLEEP_TIME="sparkbench.sleep.reducer.seconds", # set in sleep workload
    INPUT_HDFS="hibench.workload.input",
    OUTPUT_HDFS="hibench.workload.output",
    )

HiBenchPropEnvMapping={v:k for k, v in HiBenchEnvPropMapping.items()}
HiBenchPropEnvMappingMandatory={v:k for k, v in HiBenchEnvPropMappingMandatory.items()}
