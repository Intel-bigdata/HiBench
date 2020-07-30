### 1. Setup ###

 * Python 2.x(>=2.6) is required.
 
 * `bc` is required to generate the HiBench report.

 * Supported Hadoop version: Apache Hadoop 2.x, CDH5.x, HDP

 * Supported Spark version: 1.6.x, 2.0.x, 2.1.x, 2.2.x

 * Build HiBench according to [build HiBench](build-hibench.md).

 * Start HDFS, Yarn, Spark in the cluster.



### 2. Configure `hadoop.conf` ###

Hadoop is used to generate the input data of the workloads.
Create and edit `conf/hadoop.conf`：

    cp conf/hadoop.conf.template conf/hadoop.conf

Property        |      Meaning
----------------|--------------------------------------------------------
hibench.hadoop.home     |      The Hadoop installation location
hibench.hadoop.executable  |   The path of hadoop executable. For Apache Hadoop, it is /YOUR/HADOOP/HOME/bin/hadoop
hibench.hadoop.configure.dir | Hadoop configuration directory. For Apache Hadoop, it is /YOUR/HADOOP/HOME/etc/hadoop
hibench.hdfs.master       |    The root HDFS path to store HiBench data, i.e. hdfs://localhost:8020/user/username
hibench.hadoop.release    |    Hadoop release provider. Supported value: apache, cdh5, hdp

Note: For CDH and HDP users, please update `hibench.hadoop.executable`, `hibench.hadoop.configure.dir` and `hibench.hadoop.release` properly. The default value is for Apache release.


### 3. Configure `spark.conf` ###

Create and edit `conf/spark.conf`：

    cp conf/spark.conf.template conf/spark.conf

Set the below properties properly:

    hibench.spark.home            The Spark installation location
    hibench.spark.master          The Spark master, i.e. `spark://xxx:7077`, `yarn-client`


### 4. Run a workload ###
To run a single workload i.e. `wordcount`.

     bin/workloads/micro/wordcount/prepare/prepare.sh
     bin/workloads/micro/wordcount/spark/run.sh

The `prepare.sh` launches a Hadoop job to generate the input data on HDFS. The `run.sh` submits the Spark job to the cluster.
`bin/run_all.sh` can be used to run all workloads listed in conf/benchmarks.lst.

### 5. View the report ###

   The `<HiBench_Root>/report/hibench.report` is a summarized workload report, including workload name, execution duration, data size, throughput per cluster, throughput per node.

   The report directory also includes further information for debugging and tuning.

  * `<workload>/spark/bench.log`: Raw logs on client side.
  * `<workload>/spark/monitor.html`: System utilization monitor results.
  * `<workload>/spark/conf/<workload>.conf`: Generated environment variable configurations for this workload.
  * `<workload>/spark/conf/sparkbench/<workload>/sparkbench.conf`: Generated configuration for this workloads, which is used for mapping to environment variable.
  * `<workload>/spark/conf/sparkbench/<workload>/spark.conf`: Generated configuration for spark.


### 6. Input data size ###

   To change the input data size, you can set `hibench.scale.profile` in `conf/hibench.conf`. Available values are tiny, small, large, huge, gigantic and bigdata. The definition of these profiles can be found in the workload's conf file i.e. `conf/workloads/micro/wordcount.conf`

### 7. Tuning ###

Change the below properties in `conf/hibench.conf` to control the parallelism

Property        |      Meaning
----------------|--------------------------------------------------------
hibench.default.map.parallelism     |    Partition number in Spark
hibench.default.shuffle.parallelism  |   Shuffle partition number in Spark


Change the below properties to control Spark executor number, executor cores, executor memory and driver memory.

Property        |      Meaning
----------------|--------------------------------------------------------
hibench.yarn.executor.num   |   Spark executor number in Yarn mode
hibench.yarn.executor.cores  |  Spark executor cores in Yarn mode
spark.executor.memory  | Spark executor memory
spark.driver.memory    | Spark driver memory


### 8. Run xgboost workload ###

Hibench xgboost benchmark depends on the xgboost libraries to build and run. The libs are ```xgboost4j_<scala version>-<xgboost version>.jar``` and ```xgboost4j-spark_<scala version>-<xgboost version>.jar```.<br>
The relevant configurations are in ```./sparkbench/ml/pom.xml```
```
    <dependency>
      <groupId>ml.dmlc</groupId>
      <artifactId>xgboost4j_${scala.binary.version}</artifactId>
      <version>1.1.0</version>
    </dependency>
    <dependency>
      <groupId>ml.dmlc</groupId>
      <artifactId>xgboost4j-spark_${scala.binary.version}</artifactId>
      <version>1.1.0</version>
    </dependency>
```
and ```./pom.xml```
```
    <repository>
      <id>xgboostrepo</id>
      <name>XGBoost Maven Repo</name>
      <url>https://s3-us-west-2.amazonaws.com/xgboost-maven-repo/release</url>
      <releases>
        <enabled>true</enabled>
      </releases>
      <snapshots>
        <enabled>false</enabled>
      </snapshots>
    </repository>
```

#### 8.a latest xgboost release (default) ####

By default, the hibench xgboost benchmark is configured to use the latest xgboost release from https://s3-us-west-2.amazonaws.com/xgboost-maven-repo/release.<br>
To use it, simply build hibench, prepare data and run xgboost benchmark. For example,
```
$ mvn -Psparkbench -Dmodules -Pml -Dspark=2.4 -Dscala=2.12 clean package
$ bin/workloads/ml/xgboost/prepare/prepare.sh && hdfs dfs -du -s -h /HiBench/XGBoost/Input
$ bin/workloads/ml/xgboost/spark/run.sh
```

#### 8.b other xgboost releases ####

To use other xgboost releases, change the xgboost versions for xgboost4j and xgboost4j-spark to the target versions in ```./sparkbench/ml/pom.xml```. The ```scala.binary.version``` can be specified by command line parameter ```-Dscala```.<br>
e.g. to use xgboost v1.0.0, change ```<version>1.1.0</version>``` to ```<version>1.0.0</version>``` for both xgboost4j and xgboost4j-spark.<br>
If the xgboost release is from other maven repo, update the xgboostrepo url in ```./pom.xml``` as well.<br>
After that, build hibench, prepare data and run xgboost benchmark.

#### 8.c xgboost jar files ####

If you only have the xgboost jar files, just copy them to $SPARK_HOME/jars/ and update the relevant versions for xgboost4j and xgboost4j-spark in sparkbench/ml/pom.xml to get aligned.<br>
For example, if xgboost is built from source on a Linux platform, the jars will be generated and installed to ```~/.m2/repository/ml/dmlc/xgboost4j_<scala version>/<xgboost version>-SNAPSHOT/``` and ```~/.m2/repository/ml/dmlc/xgboost4j-spark_<scala version>/<xgboost version>-SNAPSHOT/``` respectively. To use them, copy the 2 jars to $SPARK_HOME/jars/ and update the relevant versions for xgboost4j and xgboost4j-spark in the pom.xml files.<br>
After that, build hibench, prepare data and run xgboost benchmark.

