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


this="${BASH_SOURCE-$0}"
bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
script="$(basename -- "$this")"
this="$bin/$script"

export HIBENCH_VERSION="2.2"

###################### Global Paths ##################

export JAVA_HOME=
export HADOOP_HOME=
export HADOOP_EXECUTABLE= 
export HADOOP_CONF_DIR=
export HADOOP_EXAMPLES_JAR=
export MAPRED_EXECUTABLE=
#Set the varaible below only in YARN mode
export HADOOP_JOBCLIENT_TESTS_JAR=

export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_VERSION=hadoop2 # set it to hadoop1 to enable MR1, hadoop2 to enable MR2

if $HADOOP_EXECUTABLE version|grep -i -q cdh4; then
	HADOOP_RELEASE=cdh4
elif $HADOOP_EXECUTABLE version|grep -i -q cdh5; then
        HADOOP_RELEASE=cdh5
elif $HADOOP_EXECUTABLE version|grep -i -q "hadoop 2"; then
        HADOOP_RELEASE=hadoop2
else
        HADOOP_RELEASE=hadoop1
fi

if [ "x"$HADOOP_VERSION == "xhadoop1" ]; then

  CONFIG_REDUCER_NUMBER=mapred.reduce.tasks
  CONFIG_MAP_NUMBER=mapred.map.tasks
else

  CONFIG_REDUCER_NUMBER=mapreduce.job.reduces
  CONFIG_MAP_NUMBER=mapreduce.job.maps
fi

echo JAVA_HOME=${JAVA_HOME:? "ERROR: Please set paths in $this before using HiBench."}
echo HADOOP_HOME=${HADOOP_HOME:? "ERROR: Please set paths in $this before using HiBench."}
echo HADOOP_EXECUTABLE=${HADOOP_EXECUTABLE:? "ERROR: Please set paths in $this before using HiBench."}
echo HADOOP_CONF_DIR=${HADOOP_CONF_DIR:? "ERROR: Please set paths in $this before using HiBench."}
echo HADOOP_EXAMPLES_JAR=${HADOOP_EXAMPLES_JAR:? "ERROR: Please set paths in $this before using HiBench."}
echo MAPRED_EXECUTABLE=${MAPRED_EXECUTABLE:? "ERROR: Please set paths in $this before using HiBench."}

if [ -z "$HIBENCH_HOME" ]; then
    export HIBENCH_HOME=`dirname "$this"`/..
fi

if [ -z "$HIBENCH_CONF" ]; then
    export HIBENCH_CONF=${HIBENCH_HOME}/conf
fi

if [ -f "${HIBENCH_CONF}/funcs.sh" ]; then
    . "${HIBENCH_CONF}/funcs.sh"
fi


if [ -z "$HIVE_HOME" ]; then
    export HIVE_HOME=${HIBENCH_HOME}/common/hive-0.12.0-bin
fi

if [ -z "$MAHOUT_HOME" ]; then
    export MAHOUT_HOME=${HIBENCH_HOME}/common/mahout-distribution-0.7-$HADOOP_RELEASE
fi

if [ -z "$NUTCH_HOME" ]; then
    export NUTCH_HOME=${HIBENCH_HOME}/nutchindexing/nutch-1.2-$HADOOP_RELEASE
fi

if [ -z "$DATATOOLS" ]; then
    export DATATOOLS=${HIBENCH_HOME}/common/autogen/dist/datatools.jar
fi

if [ $# -gt 1 ]
then
    if [ "--hadoop_config" = "$1" ]
          then
              shift
              confdir=$1
              shift
              HADOOP_CONF_DIR=$confdir
    fi
fi

# base dir HDFS
export DATA_HDFS=/HiBench

# local report
export HIBENCH_REPORT=${HIBENCH_HOME}/hibench.report

################# Compress Options #################
# swith on/off compression: 0-off, 1-on.
# Switch it off (COMPRESS_GLOBAL=0) for better performance
export COMPRESS_GLOBAL=1
export COMPRESS_CODEC_GLOBAL=org.apache.hadoop.io.compress.DefaultCodec
export COMPRESS_CODEC_MAP=org.apache.hadoop.io.compress.DefaultCodec
# Set COMPRESS_CODEC_MAP to SnappyCodec (as shown below) for better performance
#export COMPRESS_CODEC_MAP=org.apache.hadoop.io.compress.SnappyCodec 

#export COMPRESS_CODEC_GLOBAL=com.hadoop.compression.lzo.LzoCodec
#export COMPRESS_CODEC_GLOBAL=org.apache.hadoop.io.compress.SnappyCodec
