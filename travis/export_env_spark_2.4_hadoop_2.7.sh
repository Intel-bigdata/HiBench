#!/usr/bin/env bash
echo "export HADOOP_HOME=/opt/hadoop-2.7.7" >> ~/.bashrc
echo "export SPARK_HOME=/opt/spark-2.4.0-bin-hadoop2.7" >> ~/.bashrc
echo "export JAVA_OPTS=-Xmx512m" >> ~/.bashrc
source ~/.bashrc

