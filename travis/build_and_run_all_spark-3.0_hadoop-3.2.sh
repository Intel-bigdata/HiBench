#!/usr/bin/env bash

#set hadoop and spark env

cp ./travis/hibench.conf ./conf/
cp ./travis/spark.conf ./conf/
cp ./travis/hadoop.conf ./conf/
cp ./travis/hadoop-layout.sh /opt/hadoop-3.2.1/libexec
sed -i '1 i hibench.hadoop.home /opt/hadoop-3.2.1' ./conf/hadoop.conf
sed -i '1 i hibench.spark.home /opt/spark-3.0.0-bin-hadoop3.2\nhibench.spark.version spark3.0' ./conf/spark.conf
sed -i '1 i hibench.hadoop.examples.jar  ${hibench.hadoop.home}/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.2.1.jar\nhibench.hadoop.examples.test.jar  ${hibench.hadoop.home}/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.1-tests.jar\nhibench.hive.release		apache-hive-3.0.0-bin' ./conf/hibench.conf
sudo -E ./travis/configssh.sh
sudo -E ./travis/restart_hadoop_spark.sh
${HADOOP_HOME}/bin/yarn node -list 2
sudo -E ./bin/run_all.sh
