#!/bin/bash

# operation type
op=ls
#op=update

# number of partitions
partitions=

# zkHost address:port
zkHost=

# topic
topic=

# spark consumer name
consumer=

path=/consumers/$consumer/offsets/$topic

java -cp ../target/zkHelper.jar:../lib/zkclient-0.3.jar:../lib/zookeeper-3.3.4.jar:../lib/log4j-1.2.15.jar com.intel.PRCcloud.zkHelper.ZKUtil $op $zkHost $path $partitions
