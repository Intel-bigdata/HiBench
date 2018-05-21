### 1. Setup ###

 * Python 2.x(>=2.6) is required.

 * `bc` is required to generate the HiBench report.

 * Supported Hadoop version: Apache Hadoop 2.x, CDH5.x, HDP

 * Build HiBench according to [build HiBench](build-hibench.md).

 * Start HDFS, Yarn in the cluster.


### 2. Configure `hadoop.conf` ###


Create and edit `conf/hadoop.conf`：

    cp conf/hadoop.conf.template conf/hadoop.conf

Set the below properties properly:

Property        |      Meaning
----------------|--------------------------------------------------------
hibench.hadoop.home     |      The Hadoop installation location
hibench.hadoop.executable  |   The path of hadoop executable. For Apache Hadoop, it is /YOUR/HADOOP/HOME/bin/hadoop
hibench.hadoop.configure.dir | Hadoop configuration directory. For Apache Hadoop, it is /YOUR/HADOOP/HOME/etc/hadoop
hibench.hdfs.master       |    The root HDFS path to store HiBench data, i.e. hdfs://localhost:8020/user/username
hibench.hadoop.release    |    Hadoop release provider. Supported value: apache, cdh5, hdp

Note: For CDH and HDP users, please update `hibench.hadoop.executable`, `hibench.hadoop.configure.dir` and `hibench.hadoop.release` properly. The default value is for Apache release.


### 3. Run a workload ###
To run a single workload i.e. `wordcount`.

     bin/workloads/micro/wordcount/prepare/prepare.sh
     bin/workloads/micro/wordcount/hadoop/run.sh

The `prepare.sh` launches a Hadoop job to generate the input data on HDFS. The `run.sh` submits a Hadoop job to the cluster.
`bin/run_all.sh` can be used to run all workloads listed in conf/benchmarks.lst and conf/frameworks.lst.

### 4. View the report ###

   The `<HiBench_Root>/report/hibench.report` is a summarized workload report, including workload name, execution duration, data size, throughput per cluster, throughput per node.

   The report directory also includes further information for debugging and tuning.

  * `<workload>/hadoop/bench.log`: Raw logs on client side.
  * `<workload>/hadoop/monitor.html`: System utilization monitor results.
  * `<workload>/hadoop/conf/<workload>.conf`: Generated environment variable configurations for this workload.

### 5. Input data size ###

   To change the input data size, you can set `hibench.scale.profile` in `conf/hibench.conf`. Available values are tiny, small, large, huge, gigantic and bigdata. The definition of these profiles can be found in the workload's conf file i.e. `conf/workloads/micro/wordcount.conf`

### 6. Tuning ###

Change the below properties in `conf/hibench.conf` to control the parallelism.

Property        |      Meaning
----------------|--------------------------------------------------------
hibench.default.map.parallelism     |    Mapper number in hadoop
hibench.default.shuffle.parallelism  |   Reducer number in hadoop

