### Build All ###
To simply build all modules in HiBench, use the below command. This could be time consuming because the hadoopbench relies on 3rd party tools like Mahout and Nutch. The build process automatically downloads these tools for you. If you won't run these workloads, you can only build a specific framework to speed up the build process.

    mvn -Dspark=2.4 -Dscala=2.11 clean package


### Build a specific framework benchmark ###
HiBench 6.0 supports building only benchmarks for a specific framework. For example, to build the Hadoop benchmarks only, we can use the below command:

    mvn -Phadoopbench -Dspark=2.4 -Dscala=2.11 clean package

To build Hadoop and spark benchmarks

    mvn -Phadoopbench -Psparkbench -Dspark=2.4 -Dscala=2.11 clean package

Supported frameworks includs: hadoopbench, sparkbench, flinkbench, stormbench, gearpumpbench.

### Specify Scala Version ###
To specify the Scala version, use -Dscala=xxx(2.11 or 2.12). By default, it builds for scala 2.11.

    mvn -Dscala=2.11 clean package
tips:
Because some Maven plugins cannot support Scala version perfectly, there are some exceptions.

1. No matter what Scala version is specified, the module (gearpumpbench/streaming) is always built in Scala 2.11.
2. When the spark version is specified to 2.4 or higher, the module (sparkbench/streaming) is only supported for Scala 2.11.



### Specify Spark Version ###
To specify the spark version, use -Dspark=xxx(2.4 or 3.0). By default, it builds for spark 2.4

    mvn -Psparkbench -Dspark=2.4 -Dscala=2.11 clean package
tips:
when the spark version is specified to spark2.4 , the scala version will be specified to scala2.11 by
default . For example , if we want use spark2.4 and scala2.11 to build hibench. we just use the command `mvn -Dspark=2.4 clean
package` , but for spark2.4 and scala2.11 , we need use the command `mvn -Dspark=2.4 -Dscala=2.11 clean package` .
Similarly , the spark2.4 is associated with the scala2.11 by default.

### Specify Hadoop Version ###
To specify the hadoop version, use -Dhadoop=xxx(2.4 or 3.2). By default, it builds for hadoop 3.2
  
    mvn -Phadoopbench -Dhadoop=2.4 -Dhive=0.14 clean package

### Specify Hive Version ###
To specify the hive version, use -Dhive=xxx(0.14 or 3.0). By default, it builds for hive 3.0
   
    mvn -Phadoopbench -Dhadoop=2.4 -Dhive=0.14 clean package


### Build a single module ###
If you are only interested in a single workload in HiBench. You can build a single module. For example, the below command only builds the SQL workloads for Spark.

    mvn -Psparkbench -Dmodules -Psql -Dspark=2.4 -Dscala=2.11 clean package

Supported modules includes: micro, ml(machine learning), sql, websearch, graph, streaming, structuredStreaming(spark 2.0 or higher) and dal.

### Build Structured Streaming ###
For Spark 2.0 or higher versions, we add the benchmark support for Structured Streaming. This is a new module which cannot be compiled in Spark 1.6. And it won't get compiled by default even if you specify the spark version as 2.0 or higher. You must explicitly specify it like this:

    mvn -Psparkbench -Dmodules -PstructuredStreaming clean package 

### Build DAL on Spark ###
By default the dal module will not be built and needs to be enabled explicitly by adding "-Dmodules -Pdal", for example:

    mvn -Psparkbench -Dmodules -Pml -Pdal -Dspark=2.4 -Dscala=2.11 clean package

Currently there is only one workload KMeans available in DAL. To run the workload, install DAL and setup the environment by following https://github.com/intel/daal

