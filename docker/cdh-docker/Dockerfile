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


# build cdh environment on hibench-base

FROM hibench-base

USER root

#==============================
# CDH Installation
#==============================

#Add the CDH repository
COPY conf/cloudera.list /etc/apt/sources.list.d/cloudera.list
#Set preference for cloudera packages
COPY conf/cloudera.pref /etc/apt/preferences.d/cloudera.pref

#Add a Repository Key
RUN wget http://archive.cloudera.com/cdh${CDH_VERSION}/ubuntu/trusty/amd64/cdh/archive.key -O archive.key && sudo apt-key add archive.key 
RUN apt-get update

# install hadoop-yarn
RUN apt-get -y install hadoop-conf-pseudo

# install spark
RUN apt-get -y install spark-core spark-history-server spark-python

# set environment variables
ENV HADOOP_CONF_DIR /etc/hadoop/conf
ENV HADOOP_HOME /usr/lib/hadoop
ENV HADOOP_PREFIX /usr/lib/hadoop
ENV HIVE_CONF_DIR /etc/hive/conf
ENV SPARK_HOME /usr/lib/spark
ENV SPARK_MASTER_IP localhost

#Copy updated config files
COPY conf/core-site.xml /etc/hadoop/conf/core-site.xml
COPY conf/hdfs-site.xml /etc/hadoop/conf/hdfs-site.xml
COPY conf/mapred-site.xml /etc/hadoop/conf/mapred-site.xml
COPY conf/yarn-site.xml /etc/hadoop/conf/yarn-site.xml
COPY conf/spark-defaults.conf /etc/spark/conf/spark-defaults.conf
COPY scripts/hadoop-env.sh /etc/hadoop/conf/hadoop-env.sh
COPY conf/hadoop.conf /root/HiBench/conf/hadoop.conf
COPY conf/spark.conf /root/HiBench/conf/spark.conf

#Format HDFS
COPY scripts/restart_hadoop_spark.sh /usr/bin/restart_hadoop_spark.sh
RUN chmod +x /usr/bin/restart_hadoop_spark.sh
#Copy RunExample File
COPY scripts/runexample.sh /root/runexample.sh
RUN chmod +x /root/runexample.sh
