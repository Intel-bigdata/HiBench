#!/usr/bin/env bash
echo "export HADOOP_HOME=/opt/hadoop-3.2.1" >> ~/.bashrc
echo "export SPARK_HOME=/opt/spark-3.0.0-bin-hadoop3.2" >> ~/.bashrc
echo "source /opt/hadoop-3.2.1/libexec/hadoop-layout.sh" >> ~/.bashrc
echo "export JAVA_OPTS=-Xmx512m" >> ~/.bashrc
source ~/.bashrc
