# StreamingBench #
**The micro benchmark for streaming**

**<font color='red'>WARNING: UNDER CONSTRUCTION</font>**

- Current version 0.0.1
- Contact: [Ruirui Lu](mailto:ruirui.lu@intel.com), [Grace Huang](mailto:jie.huang@intel.com)
- Homepage: https://github.com/Intel-bigdata/StreamingBench/
- Current development branch: 0.0.1-draft

- Components
  1. datagen
  2. sparkbench
  3. stormbench

---
### Prequisities ###
Basically two clusters are needed, one cluster for Spark/Storm and one cluster for Kafka. It is better to have kafka nodes equal to slaves/supervisors of Spark/Storm cluster for reaching greater consumption speed. 

Datagen is deployed on every kafka node while sparkbench and stormbench are deployed on one node of Spark/Storm cluster.

Sparkbench specifies spark master in sparkbench/conf/configure.sh to associate with spark cluster. Zookeeper host information of Storm which helps to find cluster is located in the conf directory of storm installation and thus it does not need any specification in our stormbench directory. As with data generation, kafka brokers are specified in datagen/conf/configure.sh and we need to put datagen on every kafka node.

---

### Datagen ###
**Single Node Setup:**

1. Goto datagen directory

2. Use "mvn package" to build it

3. Edit "conf/configure.sh" file to set params

4. Running "bin/gendata.sh" will start generating records

**Run in cluster:**

1. Deploy this datagen in every kafka broker(as stated above)

2. Run "bin/gendata.sh" simutaneously in all brokers

**Note:**

Two data sets are provided for generation, namely text dataset and numeric dataset. The target dataset is configured in the "datagen/conf/configure.sh" file. The data set shall be configured correspondent with target benchmark to run. Only "statistics" benchmark uses numeric data set. Other benchmarks all use text data set.

---

### Sparkbench ###
**Setup:**

1. Go to sparkbench directory

2. Use "mvn package" to build it

3. Edit "conf/configure.sh" file to set params

4. Running "bin/run.sh" will start the benchmark job. Please start this "run.sh" earlier than "gendata.sh" to ensure all input is processed. The job will stop if processed records meet the target volume.

**Metric obtain:**

When the job finishes, some metrics will be output to log file and console.

1. For throughput in terms of record/s, it is logged to console  while for throughput in terms of MB/s, we shall first get run time from the output and get total bytes processed from console log of data generators and calculate by a simple division.

2. For latency, average batch processing latency is logged. We only need to add half of batch interval to this value for correction of latency.

---

### Stormbench ###
**Setup:**

1. Go to "stormbench" directory

2. Configuration files are located in two places:stormbench/conf/configure.sh(for benchmark related config) and stormbench/src/resource/config.properties(for cluster config). Modify them according to need.

3. Use "mvn package" to build it

4. Running "bin/run.sh" will start running benchmark topology. Also note to launch this "run.sh" earlier than "gendata.sh" to ensure storm receives all input.

5. Running "bin/stop" will kill the topology

**Metric obtain:**

When the topology starts, a separate thread will contact nimbus every 3 seconds to check if it has finished. If it finds the computation has ended, it outputs the total time taken.

1. For throughput, it also needs byte size processed from console log of "gendata.sh" and divide it by the determined run time.

2. For latency, although it can be obtained through api, we only refer to the storm ui page which contains the metric.
