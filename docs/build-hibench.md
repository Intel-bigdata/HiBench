
### Build All ###
To simply build all modules in Hibench, use the below command. This could be time consuming because the hadoopbench relies on 3rd party tools like Mahout and Nutch. The build process automatilly downloads these tools for you. If you won't run these workloads, you can only build a specific framework to speed up the build process.

    mvn clean package
 

### Build a specific framework benchmark ###
HiBench 6.0 supports building only bechmarks for a specific framework. For example, to build the hadoop benchmarks only, we can use the below command:

    mvn -Phadoopbench clean package

To build hadoop and spark benchmarks

    mvn -Phadoopbench -Psparkbench clean package

Supported frameworks includs: hadoopbench, sparkbench, flinkbench, stormbench, gearpumpbench.

### Specify Scala Version ###
To specify the Scala version, use -Dscala=xxx(2.10 or 2.11). By default, it builds for scala 2.10.

    mvn -Dscala=2.11 clean package
tips:
Because some maven plugins cannot support scala version perfectly, there are some exceptions. 

1. No matter what scala version is specified, the module (gearpumpbench/streaming) is always built in scala 2.11. 
2. When the spark verison is specified to 2.0, the module (sparkbench/streaming) is only supported for scala 2.11.

      

### Specify Spark Version ###
To specify the spark version, use -Dspark=xxx(1.6 or 2.0). By default, it builds for spark 1.6.   

    mvn -Psparkbench -Dspark=2.0 clean package
tips:
when the spark version is specified to spark2.0(1.6) , the scala version will be specified to scala2.11(2.10) by
default . For example , if we want use spark2.0 and scala2.11 to build hibench. we just use the command `mvn -Dspark=2.0 clean 
package` , but for spark2.0 and scala2.10 , we need use the command `mvn -Dspark=2.0 -Dscala=2.10 clean package` . 
Similarly , the spark1.6 is associated with the scala2.10 by default.

### Build a single module ###
If you are only interested in a single workload in HiBench. You can build a single module. For example, the below command only builds the sql workloads for Spark.

    mvn -Psparkbench -Dmodules -Psql clean package

Supported modules includes: micro, ml(machine learning), sql, websearch, graph, streaming.
