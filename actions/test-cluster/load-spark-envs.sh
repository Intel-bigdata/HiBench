#!/usr/bin/env bash

set -x

export HADOOP_HOME=~/opt/hadoop-$HADOOP_VERSION
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native

export SPARK_HOME=~/opt/spark-$SPARK_VERSION-bin-$SPARK_HADOOP_VERSION

export PATH=$HADOOP_HOME/bin:$SPARK_HOME/bin:$PATH

set +x
