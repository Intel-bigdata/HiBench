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
root_dir=`cd "${current_dir}/.."; pwd`

mvn clean package -q -Dmaven.javadoc.skip=true -Dspark=2.4 -Dscala=2.11
cp $root_dir/travis/spark_2.4.conf $root_dir/conf/spark.conf
sudo -E $root_dir/travis/configssh.sh
sudo -E $root_dir/travis/restart_hadoop_spark.sh
/opt/hadoop-2.7.7/bin/yarn node -list 2
sudo -E $root_dir/bin/run_all.sh
