#!/usr/bin/env bash

# exit when any command fails
set -e

# keep track of the last executed command
trap 'last_command=$current_command; current_command=$BASH_COMMAND' DEBUG
# echo an error message before exiting
trap 'echo "\"${last_command}\" command filed with exit code $?."' EXIT

HOST_NAME=$(hostname -f)
echo $HOST_NAME

# copy hibench conf
cp $GITHUB_WORKSPACE/actions/hibench.conf $GITHUB_WORKSPACE/conf/
cp $GITHUB_WORKSPACE/actions/spark.conf $GITHUB_WORKSPACE/conf/
cp $GITHUB_WORKSPACE//actions/hadoop.conf $GITHUB_WORKSPACE/conf/

# set hadoop path , spark path and dependency jar
sed -i '1 i hibench.hadoop.home ~/opt/hadoop-2.7.7' $GITHUB_WORKSPACE/conf/hadoop.conf
sed -i "1 i hhibench.hdfs.master hdfs://${HOST_NAME}:8020" $GITHUB_WORKSPACE/conf/hadoop.conf
cat $GITHUB_WORKSPACE/conf/hadoop.conf
sed -i '1 i hibench.spark.home ~/opt/spark-2.4.0-bin-hadoop2.7\nhibench.spark.version spark2.4' $GITHUB_WORKSPACE/conf/spark.conf
sed -i '1 i hibench.hadoop.examples.jar  ${hibench.hadoop.home}/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.7.jar\nhibench.hadoop.examples.test.jar  ${hibench.hadoop.home}/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.7.7-tests.jar\nhibench.hive.release		apache-hive-0.14.0-bin' $GITHUB_WORKSPACE/conf/hibench.conf

set +x
