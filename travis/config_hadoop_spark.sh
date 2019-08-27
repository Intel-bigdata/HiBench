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

# Copy configuration depending on the Java, Spark and Hadoop versions.
if [[ "$java_ver" == 11 ]]; then
    cp ./travis/artifacts/hadoop32/spark-env.sh /opt/$SPARK_BINARIES_FOLDER/conf/
    cp ./travis/artifacts/hadoop32/hadoop-env.sh $HADOOP_CONF_DIR
    # Java 11 removed java.activation module completely. [JEP 320](http://openjdk.java.net/jeps/320)
    # Including it in Hadoop in order to enable Hadoop 3.2.0 using Java 11.
    URL=https://jcenter.bintray.com/javax/activation/javax.activation-api/1.2.0/javax.activation-api-1.2.0.jar
    wget $URL -P $HADOOP_HOME/share/hadoop/common
    echo 'export HADOOP_CLASSPATH+=" $HADOOP_HOME/share/hadoop/common/*.jar"' >> $HADOOP_CONF_DIR/hadoop-env.sh
    cp ./travis/artifacts/hadoop32/mapred-site.xml $HADOOP_CONF_DIR
    cp ./travis/artifacts/hadoop32/yarn-site.xml $HADOOP_CONF_DIR
    sed -i "s|<maven.compiler.source>1.6</maven.compiler.source>|<maven.compiler.source>11</maven.compiler.source>|g" pom.xml
    sed -i "s|<maven.compiler.target>1.6</maven.compiler.target>|<maven.compiler.target>11</maven.compiler.target>|g" pom.xml
    sed -i "s|sql.scan|#sql.scan|g" ./travis/benchmarks.lst
elif [[ "$java_ver" == 8 ]]; then
    cp ./travis/artifacts/hadoop32/spark-env.sh /opt/$SPARK_BINARIES_FOLDER/conf/
    cp ./travis/artifacts/hadoop32/hadoop-env.sh $HADOOP_CONF_DIR
    cp ./travis/artifacts/hadoop32/mapred-site.xml $HADOOP_CONF_DIR
    cp ./travis/artifacts/hadoop32/yarn-site.xml $HADOOP_CONF_DIR
    sed -i "s|<maven.compiler.source>1.6</maven.compiler.source>|<maven.compiler.source>1.8</maven.compiler.source>|g" pom.xml
    sed -i "s|<maven.compiler.target>1.6</maven.compiler.target>|<maven.compiler.target>1.8</maven.compiler.target>|g" pom.xml
    sed -i "s|sql.scan|#sql.scan|g" ./travis/benchmarks.lst
elif [[ "$java_ver" == 7 ]]; then
    cp ./travis/artifacts/hadoop26/spark-env.sh /opt/$SPARK_BINARIES_FOLDER/conf/
    cp ./travis/artifacts/hadoop26/mapred-site.xml $HADOOP_CONF_DIR
    cp ./travis/artifacts/hadoop26/yarn-site.xml $HADOOP_CONF_DIR
    sed -i "s|<maven.compiler.source>1.6</maven.compiler.source>|<maven.compiler.source>1.7</maven.compiler.source>|g" pom.xml
    sed -i "s|<maven.compiler.target>1.6</maven.compiler.target>|<maven.compiler.target>1.7</maven.compiler.target>|g" pom.xml
else
    echo "No configuration setting for this Java version"
fi

# Copy common configuration files to hadoop conf dir
cp ./travis/artifacts/core-site.xml $HADOOP_CONF_DIR
cp ./travis/artifacts/hdfs-site.xml $HADOOP_CONF_DIR

# Copy Hi-bench configuration
cp ./travis/hibench.conf ./conf/
cp ./travis/benchmarks.lst ./conf/
cp ./travis/hadoop.conf ./conf/
cp ./travis/spark.conf ./conf/

# Set the actual version of hadoop and spark in Hi-bench config files
sed -i "s|/opt/|/opt/$HADOOP_BINARIES_FOLDER|g" ./conf/hadoop.conf
sed -i "s|/opt/|/opt/$SPARK_BINARIES_FOLDER/|g" ./conf/spark.conf
sed -i "s|sparkx.x|spark$(echo $SPARK_VER | cut -d. -f-2)|g" ./conf/spark.conf
