#!/usr/bin/env bash

# exit when any command fails
set -e

# keep track of the last executed command
trap 'last_command=$current_command; current_command=$BASH_COMMAND' DEBUG
# echo an error message before exiting
trap 'echo "\"${last_command}\" command filed with exit code $?."' EXIT

HOST_NAME=$(hostname -f)

# copy hibench conf
cp $GITHUB_WORKSPACE/actions/hibench.conf $GITHUB_WORKSPACE/conf/
cp $GITHUB_WORKSPACE/actions/spark.conf $GITHUB_WORKSPACE/conf/
cp $GITHUB_WORKSPACE/actions/hadoop.conf $GITHUB_WORKSPACE/conf/
cp $GITHUB_WORKSPACE/actions/hadoop-layout.sh ~/opt/hadoop-${HADOOP_VERSION}/libexec
# set hadoop path , spark path and dependency jar
sed -i "1 i hibench.hadoop.home ~/opt/hadoop-${HADOOP_VERSION}" $GITHUB_WORKSPACE/conf/hadoop.conf
sed -i "1 i hibench.hdfs.master hdfs://${HOST_NAME}:8020" $GITHUB_WORKSPACE/conf/hadoop.conf
sed -i "1 i hibench.spark.home ~/opt/spark-${SPARK_VERSION}-bin-${SPARK_HADOOP_VERSION}\nhibench.spark.version ${SPARK_BIN_VERSION}" $GITHUB_WORKSPACE/conf/spark.conf
sed -i "1 i hibench.hadoop.examples.jar  ${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-examples-${HADOOP_VERSION}.jar\nhibench.hadoop.examples.test.jar  ${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-${HADOOP_VERSION}-tests.jar\nhibench.hive.release		apache-hive-${HIVE_VERSION}-bin" $GITHUB_WORKSPACE/conf/hibench.conf

set +x
