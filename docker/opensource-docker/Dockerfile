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


# build opensource environment on hibench-base

FROM hibench-base

USER root

#==============================
# HADOOP Installation
#==============================

# environment variables for HADOOP
ENV HADOOP_HOME /usr/local/hadoop-${HADOOP_VERSION_DETAIL}
ENV HADOOP_PREFIX /usr/local/hadoop-${HADOOP_VERSION_DETAIL}
ENV HADOOP_CONF_DIR ${HADOOP_HOME}/etc/hadoop/

RUN export HADOOP_INSTALL=$HADOOP_HOME
RUN export PATH=$PATH:$HADOOP_INSTALL/bin
RUN export PATH=$PATH:$HADOOP_INSTALL/sbin
RUN export HADOOP_MAPRED_HOME=$HADOOP_INSTALL
RUN export HADOOP_COMMON_HOME=$HADOOP_INSTALL
RUN export HADOOP_HDFS_HOME=$HADOOP_INSTALL
RUN export YARN_HOME=$HADOOP_INSTALL
RUN export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_INSTALL/lib/native
RUN export HADOOP_OPTS="-Djava.library.path=$HADOOP_INSTALL/lib"

RUN wget http://apache.mirrors.lucidnetworks.net/hadoop/common/hadoop-${HADOOP_VERSION_DETAIL}/hadoop-${HADOOP_VERSION_DETAIL}.tar.gz
RUN tar xzf hadoop-*.tar.gz -C /usr/local/
RUN rm -f hadoop-*.tar.gz 


#==============================
# SPARK Installation
#==============================

# spark home environments
ENV SPARK_HOME /usr/local/spark-${SPARK_VERSION_DETAIL}
ENV SPARK_MASTER_IP localhost

# download spark
RUN wget http://d3kbcqa49mib13.cloudfront.net/spark-${SPARK_VERSION_DETAIL}-bin-hadoop${HADOOP_FOR_SPARK_VERSION}.tgz 
RUN tar xzf spark-*.tgz -C /usr/local
RUN mv /usr/local/spark-* ${SPARK_HOME}
RUN rm -f spark-*.tgz 

# Copy updated config files
COPY conf/core-site.xml ${HADOOP_CONF_DIR}/
COPY conf/hdfs-site.xml ${HADOOP_CONF_DIR}/
COPY conf/mapred-site.xml ${HADOOP_CONF_DIR}/
COPY conf/yarn-site.xml ${HADOOP_CONF_DIR}/
COPY scripts/hadoop-env.sh ${HADOOP_CONF_DIR}/
COPY scripts/restart-hadoop-spark.sh /usr/bin
RUN chmod +x /usr/bin/restart-hadoop-spark.sh
#Copy RunExample File
COPY scripts/runexample.sh /root/runexample.sh
RUN chmod +x /root/runexample.sh
