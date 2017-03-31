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



# start ssh service
service ssh restart

# remove existing data directories
rm -fr /var/lib/hadoop-hdfs/*
sudo -u hdfs hdfs namenode -format

sudo -u hdfs chmod -R 777 /var/lib/hadoop-hdfs

# start hadoop nodes
service hadoop-hdfs-namenode restart
service hadoop-hdfs-datanode restart

#sudo -u hdfs hdfs dfs -mkdir -p /tmp/hadoop-yarn/staging/history/done_intermediate
hdfs dfs -mkdir -p /tmp/hadoop-yarn/staging/history/done_intermediate
hdfs dfs -chown -R mapred:mapred /tmp/hadoop-yarn/staging
hdfs dfs -chmod -R 1777 /tmp
hdfs dfs -mkdir -p /var/log/hadoop-yarn
hdfs dfs -chown yarn:mapred /var/log/hadoop-yarn

# start yarn
service hadoop-yarn-resourcemanager restart
service hadoop-yarn-nodemanager restart
service hadoop-mapreduce-historyserver restart

hdfs dfs -mkdir -p /user/hdfs
hdfs dfs -chown hdfs:hdfs /user/hdfs

# start spark-history-server
hdfs dfs -mkdir -p /user/spark/applicationHistory 
hdfs dfs -chown -R spark:spark /user/spark
hdfs dfs -chmod 1777 /user/spark/applicationHistory

service spark-history-server restart
${SPARK_HOME}/sbin/stop-all.sh
${SPARK_HOME}/sbin/start-all.sh

echo "#======================================================================#"
echo "       Now you can run all workload by:                                 "
echo "                                                                        "
echo "         ${HIBENCH_HOME}/bin/run_all.sh         "
echo "#======================================================================#"
