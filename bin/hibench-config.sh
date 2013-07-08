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


#++++++++++++ Paths and options (configurable) ++++++++++++
# basic paths
HADOOP_EXECUTABLE= 
HADOOP_CONF_DIR=
HADOOP_EXAMPLES_JAR=

# dict path
DICT_PATH=

# hdfs data path
DATA_HDFS=

# report file path
HIBENCH_REPORT=

# compress: 0-off, 1-on
COMPRESS_GLOBAL=
COMPRESS_CODEC_GLOBAL=
#COMPRESS_CODEC_GLOBAL=org.apache.hadoop.io.compress.DefaultCodec
#COMPRESS_CODEC_GLOBAL=com.hadoop.compression.lzo.LzoCodec
#COMPRESS_CODEC_GLOBAL=org.apache.hadoop.io.compress.SnappyCodec
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


#+++++++++++++++ Set/guess paths or values ++++++++++++++++
# get conf dir if command option defined
if [ $# -gt 1 ]; then
    if [ "--hadoop_config" = "$1" ]; then
        shift
        confdir=$1
        shift
        HADOOP_CONF_DIR=$confdir
    fi
fi

# check and set basic paths
if [ -n "$HADOOP_HOME" ]; then
    HADOOP_EXECUTABLE=$HADOOP_HOME/bin/hadoop
    if [ -z $HADOOP_CONF_DIR ]; then
        HADOOP_CONF_DIR=$HADOOP_HOME/conf
    fi
    if [ -z $HADOOP_EXAMPLES_JAR ]; then
        HADOOP_EXAMPLES_JAR=`ls $HADOOP_HOME/hadoop-examples*.jar | head -1`
    fi
else 					
    ## guess basic paths if they are not set
    if [ -z $HADOOP_EXECUTABLE ]; then
       	HADOOP_EXECUTABLE=`which hadoop`
    fi
    IFS=':'
    for d in `$HADOOP_EXECUTABLE classpath`; do
        if [ -z $HADOOP_CONF_DIR ] && [[ $d = */conf ]]; then
            HADOOP_CONF_DIR=$d
       	fi
        if [ -z $HADOOP_EXAMPLES_JAR ] && [[ $d = *hadoop-examples*.jar ]]; then
            HADOOP_EXAMPLES_JAR=$d
        fi
    done
    unset IFS
fi

echo HADOOP_EXECUTABLE=${HADOOP_EXECUTABLE:? "ERROR: Please set paths in $this before using HiBench."}
echo HADOOP_CONF_DIR=${HADOOP_CONF_DIR:? "ERROR: Please set paths in $this before using HiBench."}
echo HADOOP_EXAMPLES_JAR=${HADOOP_EXAMPLES_JAR:? "ERROR: Please set paths in $this before using HiBench."}

# check dict path and dict file
if [ -z "$DICT_PATH" ]; then
    DICT_PATH=/usr/share/dict/words
fi

if [[ ! -e "$DICT_PATH" ]]; then
    echo "ERROR: Dict file ${DICT_PATH} does not exist!"
    exit
fi

count=`cat $DICT_PATH | wc -w`
if (($count < 20)); then
    echo "ERROR: The number of dict words in ${DICT_PATH} is less than 20!"
    exit
fi

# check HiBench basic paths
if [ -z "$HIBENCH_HOME" ]; then
    HIBENCH_HOME=`dirname "$bin"`
fi

if [ -z "$HIBENCH_CONF" ]; then
    HIBENCH_CONF=${HIBENCH_HOME}/conf
fi

if [ -f "${HIBENCH_CONF}/funcs.sh" ]; then
    . "${HIBENCH_CONF}/funcs.sh"
fi

# check and set default HDFS path of data
if [ -z "$DATA_HDFS" ]; then
    DATA_HDFS=/HiBench
fi

# check and set default path of report file
if [ -z "$HIBENCH_REPORT" ]; then
    HIBENCH_REPORT=${HIBENCH_HOME}/hibench.report
fi

# check and set default compress options
if [ -z "$COMPRESS_GLOBAL" ]; then
    COMPRESS_GLOBAL=1
fi

if [ -z "$COMPRESS_CODEC_GLOBAL" ]; then
    COMPRESS_CODEC_GLOBAL=org.apache.hadoop.io.compress.DefaultCodec
fi

# internal paths (DO NOT CHANGE UNLESS NECESSARY)
if $HADOOP_EXECUTABLE version|grep -i -q cdh4; then
    HADOOP_VERSION=cdh4
else
    HADOOP_VERSION=hadoop1
fi
MAHOUT_HOME=${HIBENCH_HOME}/common/mahout-distribution-0.7-$HADOOP_VERSION
NUTCH_HOME=${HIBENCH_HOME}/nutchindexing/nutch-1.2-$HADOOP_VERSION
DATATOOLS=${HIBENCH_HOME}/common/autogen/dist/datatools.jar
HIVE_HOME=${HIBENCH_HOME}/common/hive-0.9.0-bin
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

