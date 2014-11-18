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

export SPARKBENCH_VERSION="0.1"

SPARKBENCH_CONF=$bin/../conf
if [[ ! -e "$SPARKBENCH_CONF" ]]; then
    echo "ERROR: $SPARKBENCH_CONF does not exist, please create one according to sparkbench-config.sh.template!"
exit
fi

if [ -f "$SPARKBENCH_CONF/sparkbench-config.sh" ]; then
	. "$SPARKBENCH_CONF/sparkbench-config.sh"
fi

# default Mappers and reducers
NUM_MAPS_default_parallelism=`grep spark.default.parallelism $SPARKBENCH_HOME/conf/global_properties.conf | tr -s ' ' |cut -d ' ' -f 2`
NUM_MAPS=${NUM_MAPS:-$NUM_MAPS_default_parallelism}
NUM_MAPS=${NUM_MAPS:-16}  # default parallelism is 16 if not defined in "global_properties.conf" and "sparkbench-config.sh"
NUM_REDS=${NUM_REDS:-$(( $NUM_MAPS/2 ))}

if [ -n "$HADOOP_HOME" ]; then
	HADOOP_EXECUTABLE=$HADOOP_HOME/bin/hadoop
	HADOOP_CONF_DIR=$HADOOP_HOME/conf
	HADOOP_EXAMPLES_JAR=$HADOOP_HOME/hadoop-examples*.jar
else 					
##make some guess if none of these variables are set
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

if [ -f "${SPARKBENCH_CONF}/funcs.sh" ]; then
    . "${SPARKBENCH_CONF}/funcs.sh"
fi

if $HADOOP_EXECUTABLE version|grep -i -q cdh4; then
	HADOOP_VERSION=cdh4
else
	HADOOP_VERSION=hadoop1
fi

if [ -z "$MAHOUT_HOME" ]; then
    export MAHOUT_HOME=${HIBENCH_HOME}/common/hibench/mahout/target/mahout-distribution-0.7
fi

if [ -z "$NUTCH_HOME" ]; then
    export NUTCH_HOME=${HIBENCH_HOME}/nutchindexing/nutch-1.2-$HADOOP_VERSION
fi

if [ -z "$DATATOOLS" ]; then
    export DATATOOLS=${HIBENCH_HOME}/common/autogen/dist/datatools.jar
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
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/conf}"

################# Compress Options #################
# swith on/off compression: 0-off, 1-on
export COMPRESS_GLOBAL=0
export COMPRESS_CODEC_GLOBAL=org.apache.hadoop.io.compress.DefaultCodec

