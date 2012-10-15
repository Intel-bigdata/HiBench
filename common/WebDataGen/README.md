# Web Data Generator 0.0.1 #
##Overview##
This is the project to automatically generate Web data with given number of Html Web pages and/or faked user visist history records. All the data are not meaningful in real world and are used in HiBench-2.1 to indicating the hadoop capability for processing some Web based workloads. At the moment this project only supports to generate data for hive and pagerank benchmarks.

##How to build##

1. set `HADOOP_HOME` variable:
    `export HADOOP_HOME=${HOME}/hadoop-0.20.2-cdh3u4`

2. make:
    `ant`

