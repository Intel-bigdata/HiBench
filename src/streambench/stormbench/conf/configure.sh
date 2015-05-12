# the bin directory of storm (storm command will be used)
STORM_BIN_HOME=

###############

#Cluster config
# nimbus of storm cluster
nimbus=
nimbusAPIPort=6627
# time interval to contact nimbus to judge if finished
nimbusContactInterval=
# zookeeper host location
zkHost=

###############

#Parallel config
# number of workers of Storm. Number of most bolt threads is also equal to this param.
workerCount=

# number of kafka spout threads of Storm
spoutThreads=

# number of bolt threads altogether
boltThreads=

# kafka arg indicating whether to read data from kafka from the start or go on to read from last position
readFromStart=false

# whether to turn on ack
ackon=true
###############

#Benchmark args
#Note to ensure benchName to be consistent with datagen type. Numeric data for statistics and text data for others
# please  uncomment one benchName to run the benchmark
#benchName=micro-identity
#benchName=micro-sample
#benchName=micro-sketch
#benchName=micro-grep
#benchName=micro-wordcount
#benchName=micro-distinctcount
#benchName=micro-statisticssep
#benchName=trident-identity
#benchName=trident-sample
#benchName=trident-sketch
#benchName=trident-grep
#benchName=trident-wordcount
#benchName=trident-distinctcount
#benchName=trident-statistics

#common args
# the topic that storm will receive input data
topic=
# consumer group of the storm consumer for kafka
consumer=
# target number of records to be processed
recordCount=

#sketch/wordcount/distinctcount/statistics arg
# the seperator between fields of a single record 
separator=\\s+

#sketch/distinctcount/statistics arg
# the field index of the record that will be extracted
fieldIndex=1

#sample arg
# probability that a record will be taken as a sample 
prob=0.1

#grep arg
# the substring that will be checked to see if contained in a record
pattern=the


