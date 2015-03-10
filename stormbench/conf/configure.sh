# the bin directory of storm (storm command will be used)
STORM_BIN_HOME=

###############

#Cluster config
# nimbus of storm cluster
nimbus=
nimbusAPIPort=6627
# zookeeper host location
zkHost=

###############

#Parallel config
# number of workers of Storm. Number of most bolt threads is also equal to this param.
workerCount=

# number of kafka spout threads of Storm
spoutThreads=

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
