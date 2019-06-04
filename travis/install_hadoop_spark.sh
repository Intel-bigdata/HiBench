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

# Download binaries
SPARK_URL=https://archive.apache.org/dist/spark/spark-$SPARK_VER/spark-$SPARK_VER-bin-$SPARK_PACKAGE_TYPE.tgz
HADOOP_URL=https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VER/hadoop-$HADOOP_VER.tar.gz
echo $SPARK_URL
echo $HADOOP_URL
cd /opt
wget $SPARK_URL
wget $HADOOP_URL
# Uncompress tarballs
tar -xzf /opt/$(ls /opt | grep $SPARK_VER)
tar -xzf /opt/$(ls /opt | grep $HADOOP_VER)
