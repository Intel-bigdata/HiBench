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

"""
Mapping from properties to environment variable names
"""
HiBenchEnvPropMappingMandatory=dict(
    HADOOP_HOME="hibench.hadoop.home",
    SPARK_HOME="hibench.spark.home",
    HDFS_MASTER="hibench.hdfs.master",
    SPARK_MASTER="hibench.spark.master",
    HADOOP_VERSION="hibench.hadoop.version",       
    HADOOP_RELEASE="hibench.hadoop.release",        
    HADOOP_EXAMPLES_JAR="hibench.hadoop.examples.jar", 
    HADOOP_EXECUTABLE="hibench.hadoop.executable", 
    HADOOP_CONF_DIR="hibench.hadoop.configure.dir",
    SPARK_VERSION="hibench.spark.version",
    HIBENCH_HOME="hibench.home",
    HIBENCH_CONF="hibench.configure.dir", 

    DEPENDENCY_DIR="hibench.dependency.dir",
    REPORT_COLUMN_FORMATS="hibench.report.formats",
    SPARKBENCH_JAR="hibench.sparkbench.jar",
    HIBENCH_PYTHON_PATH="hibench.sparkbench.python.dir",
    NUM_MAPS="hibench.default.map.parallelism",
    NUM_REDS="hibench.default.shuffle.parallelism",
    INPUT_HDFS="hibench.workload.input",
    OUTPUT_HDFS="hibench.workload.output",

    REDUCER_CONFIG_NAME="hibench.hadoop.reducer.name",
    MAP_CONFIG_NAME="hibench.hadoop.mapper.name",
    COMPRESS_OPT="hibench.workload.compress.options",
    DATATOOLS_COMPRESS_OPT="hibench.workload.compress.datatools.options",
    KMEANS_COMPRESS_OPT="hibench.workload.compress.kmeans_gen.options",
    HIVE_SQL_COMPRESS_OPTS="hibench.workload.compress.hive.options",

    MASTERS="hibench.masters.hostnames",
    SLAVES="hibench.slaves.hostnames",
    )

HiBenchEnvPropMapping=dict(
    SPARK_EXAMPLES_JAR="hibench.spark.examples.jar",

    HIVE_HOME="hibench.hive.home",
    HIVE_RELEASE="hibench.hive.release",
    HIVEBENCH_TEMPLATE="hibench.hivebench.template.dir",
    MAHOUT_HOME="hibench.mahout.home",
    MAHOUT_RELEASE="hibench.mahout.release",
    NUTCH_HOME="hibench.nutch.home",
    NUTCH_BASE_HDFS="hibench.nutch.base.hdfs",
    NUTCH_INPUT="hibench.nutch.dir.name.input",
    NUTCH_DIR="hibench.nutch.nutchindexing.dir",
    HIBENCH_REPORT="hibench.report.dir", # set in default
    HIBENCH_REPORT_NAME="hibench.report.name", # set in default
    YARN_NUM_EXECUTORS="hibench.yarn.executor.num",
    YARN_EXECUTOR_CORES="hibench.yarn.executor.cores",
    YARN_EXECUTOR_MEMORY="hibench.yarn.executor.memory",
    YARN_DRIVER_MEMORY="hibench.yarn.driver.memory",
    DATA_HDFS="hibench.hdfs.data.dir",
    # For Sleep workload
    MAP_SLEEP_TIME="hibench.sleep.mapper.seconds",
    RED_SLEEP_TIME="hibench.sleep.reducer.seconds",
    HADOOP_SLEEP_JAR="hibench.sleep.job.jar",
    # For Sort, Terasort, Wordcount
    DATASIZE="hibench.workload.datasize",
    BYTES_TOTAL_NAME="hibench.randomtextwriter.bytestotal.name",

    # For hive related workload, data scale
    PAGES="hibench.workload.pages",
    USERVISITS="hibench.workload.uservisits",
    HIVE_INPUT="hibench.hive.dir.name.input",
    HIVE_BASE_HDFS="hibench.hive.base.hdfs",
    # For bayes
    CLASSES="hibench.workload.classes",
    BAYES_INPUT="hibench.bayes.dir.name.input",
    DATATOOLS="hibench.hibench.datatool.dir",
    BAYES_BASE_HDFS="hibench.bayes.base.hdfs",
    NGRAMS="hibench.bayes.ngrams",
    # For kmeans
    INPUT_SAMPLE="hibench.kmeans.input.sample",
    INPUT_CLUSTER="hibench.kmeans.input.cluster",
    NUM_OF_CLUSTERS="hibench.kmeans.num_of_clusters",
    NUM_OF_SAMPLES="hibench.kmeans.num_of_samples",
    SAMPLES_PER_INPUTFILE="hibench.kmeans.samples_per_inputfile",
    DIMENSIONS="hibench.kmeans.dimensions",
    MAX_ITERATION="hibench.kmeans.max_iteration",
    K="hibench.kmeans.k",
    # For Pagerank
    PAGERANK_BASE_HDFS="hibench.pagerank.base.hdfs",
    PAGERANK_INPUT="hibench.pagerank.dir.name.input",
    BLOCK="hibench.pagerank.block",
    NUM_ITERATIONS="hibench.pagerank.num_iterations",
    PEGASUS_JAR="hibench.pagerank.pegasus.dir",
    # For DFSIOE
    RD_NUM_OF_FILES="hibench.dfsioe.read.number_of_files",
    RD_FILE_SIZE="hibench.dfsioe.read.file_size",
    WT_NUM_OF_FILES="hibench.dfsioe.write.number_of_files",
    WT_FILE_SIZE="hibench.dfsioe.write.file_size",
    MAP_JAVA_OPTS="hibench.dfsioe.map.java_opts",
    RED_JAVA_OPTS="hibench.dfsioe.red.java_opts",
    )

HiBenchPropEnvMapping=dict([(v,k) for k, v in HiBenchEnvPropMapping.items()])
HiBenchPropEnvMappingMandatory=dict([(v,k) for k, v in HiBenchEnvPropMappingMandatory.items()])
