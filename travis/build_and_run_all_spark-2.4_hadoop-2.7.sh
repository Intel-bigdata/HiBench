#!/usr/bin/env bash

mvn clean package -q -Dmaven.javadoc.skip=true -Dspark=2.4 -Dscala=2.11 -Dhive=0.14 -Dhadoop=2.4
cp ./travis/hibench.conf ./conf/
cp ./travis/spark.conf ./conf/
cp ./travis/hadoop.conf ./conf/
sed -i '1 i hibench.hadoop.home /opt/hadoop-2.7.7' ./conf/hadoop.conf
sed -i '1 i hibench.spark.home /opt/spark-2.4.0-bin-hadoop2.7\nhibench.spark.version spark2.4' ./conf/spark.conf
sed -i '1 i hibench.hadoop.examples.jar  ${hibench.hadoop.home}/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.7.jar\nhibench.hadoop.examples.test.jar  ${hibench.hadoop.home}/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.7.7-tests.jar\nhibench.hive.release		apache-hive-0.14.0-bin' ./conf/hibench.conf
sudo -E ./travis/configssh.sh
sudo -E ./travis/restart_hadoop_spark.sh
${HADOOP_HOME}/bin/yarn node -list 2
sudo -E ./bin/run_all.sh
