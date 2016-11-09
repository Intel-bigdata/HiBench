### 1. Setup ###
 (1) Setup JDK, HDFS, Yarn properly.

 (2) Python 2.x(>=2.6) is required.

 (3) Build HiBench with Maven. 

     mvn clean package


### 2. Configure `hadoop.conf` ###


Create & edit `conf/hadoop.conf`：

    cp conf/hadoop.conf.template hadoop.conf

Set the below properties properly:

    hibench.hadoop.home           The Hadoop installation location
    hibench.hadoop.executable     The path of hadoop executable. For Apache Hadoop, it is <YOUR/HADOOP/HOME>/bin/hadoop
    hibench.hadoop.configure.dir  Hadoop configuration directory. For Apache Hadoop, it is <YOUR/HADOOP/HOME>/etc/hadoop
    hibench.hdfs.master           The root HDFS path to store HiBench data, i.e. hdfs://localhost:8020/user/username
    hibench.hadoop.release        Hadoop release provider. Supported value: apache, cdh5, hdp


### 3. Run a workload ###
To run a single workload i.e. `wordcount`. 

     bin/workloads/micro/wordcount/prepare/prepare.sh
     bin/workloads/micro/wordcount/hadoop/run.sh

The `prepare.sh` launchs a hadoop job to genereate the input data on HDFS. The `run.sh` submits the hadoop job to the cluster. 
`bin/run-all.sh` can be used to run all workloads listed in conf/benchmarks.lst and conf/frameworks.lst.

### 4. View the report ###

   The `<HiBench_Root>/report/hibench.report` is a summarized workload report, including workload name, execution duration, data size, throughput per cluster, throughput per node.

   The report directory also includs further information for debuging and tuning.
     
  * `<workload>/hadoop/bench.log`: Raw logs on client side.
  * `<workload>/hadoop/monitor.html`: System utilization monitor results.
  * `<workload>/hadoop/conf/<workload>.conf`: Generated environment variable configurations for this workload.
