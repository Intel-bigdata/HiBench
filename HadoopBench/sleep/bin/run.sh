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
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

echo "========== running sleep bench =========="
# configure
DIR=`cd $bin/../; pwd`
. "${DIR}/../bin/hibench-config.sh"
. "${DIR}/conf/configure.sh"

START_TIME=`timestamp`

#run bench
if [ $HADOOP_RELEASE == "hadoop1" ]; then
  $HADOOP_EXECUTABLE jar $HADOOP_EXAMPLES_JAR sleep \
      -m $NUM_MAPS \
      -r $NUM_REDS \
      -mt $MAP_SLEEP_TIME \
      -mr $RED_SLEEP_TIME
else
  $HADOOP_EXECUTABLE jar $HADOOP_JOBCLIENT_TESTS_JAR sleep \
      -m $NUM_MAPS \
      -r $NUM_REDS \
      -mt $MAP_SLEEP_TIME \
      -mr $RED_SLEEP_TIME
fi

#post-running
END_TIME=`timestamp`

SIZE="0"

gen_report "SLEEP" ${START_TIME} ${END_TIME} ${SIZE}
