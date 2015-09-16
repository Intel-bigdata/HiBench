#!/bin/bash

# operation type
op=ls
#op=update

# number of partitions
partitions=48

# zkHost address:port
zkHost=lv-dev:2181

# topic
topic=identity-source-60

# spark consumer name
consumer=xxx

path=/consumers/$consumer/offsets/$topic

java -cp ../target/zkHelper.jar:../lib/zkclient-0.3.jar:../lib/zookeeper-3.3.4.jar:../lib/log4j-1.2.15.jar com.intel.PRCcloud.zkHelper.ZKUtil $op $zkHost $path $partitions
