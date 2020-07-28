#!/usr/bin/env bash

#set hadoop and spark env

cp ./travis/hibench.conf ./conf/
cp ./travis/spark.conf ./conf/
cp ./travis/hadoop.conf ./conf/
sed -i '1 i hibench.hadoop.home /opt/hadoop-2.7.7' ./conf/hadoop.conf
sed -i '1 i hibench.spark.home /opt/spark-3.0.0-bin-hadoop2.7\nhibench.spark.version spark3.0' ./conf/spark.conf
sed -i '1 i hibench.hadoop.examples.jar  ${hibench.hadoop.home}/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.7.jar\nhibench.hadoop.examples.test.jar  ${hibench.hadoop.home}/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.7.7-tests.jar' ./conf/hibench.conf
echo "export HADOOP_HOME=/opt/hadoop-2.7.7" >> /home/travis/.branchrc
echo "export SPARK_HOME=/opt/spark-3.0.0-bin-hadoop2.7" >> /home/travis/.branchrc
source /home/travis/.branchrc