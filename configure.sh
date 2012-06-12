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

#!/bin/bash
export HIBENCH_VERSION="2.1"

###################### Global Paths ##################
if [ -z "$HIBENCH_HOME" ]; then
    export HIBENCH_HOME=/home/${USER}/HiBench-${HIBENCH_VERSION}
fi

if [ -z "$HADOOP_HOME" ]; then
    export HADOOP_HOME=/home/${USER}/hadoop-1.0.2
fi

if [ -z "$HIVE_HOME" ]; then
    export HIVE_HOME=${HIBENCH_HOME}/common/hive-0.9.0-bin
fi

if [ -z "$MAHOUT_HOME" ]; then
    export MAHOUT_HOME=${HIBENCH_HOME}/common/mahout-distribution-0.6
fi

export HADOOP_CONF_DIR=$HADOOP_HOME/conf
export DATA_HDFS=/HiBench
export DATA_LOCAL=${HIBENCH_HOME}/../HiBench-Data
export HIBENCH_REPORT=${HIBENCH_HOME}/hibench.report

################# Compress Options #################
# swith on/off compression: 0-off, 1-on
export COMPRESS_GLOBAL=1
export COMPRESS_CODEC_GLOBAL=org.apache.hadoop.io.compress.DefaultCodec
