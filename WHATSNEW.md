We are happy to announce HiBench-4.0, a major release with a totally new design. There're lots of usability  enhancements and development improvements, new features, new platform supports!

## We are now support multiple language APIs ##

Spark is a fast and general engine for large-scale data processing framework, which has multiple language backends. HiBench is now supporting MapReduce, Spark/Scala, Spark/Java, Spark/Python for all workloads (except for nutchindexing & dfsioe).

## Unified and auto-probe configurations ##

In this version, all you need is set HiBench *hadoop home*, *spark home*, *hdfs master* and *spark master*. HiBench will detect hadoop/spark releases and versions, and infer configures for you. There'll be no need to re-build HiBench if you switch between MR1 with MR2, or Spark1.2 with Spark1.3.

## Explicitly report ##

Now, the reported information is greatly enriched. For each workload, each language API, an all-in-one configuration file will be generated in report folder, with hints about where/how the config values come from, along with the related logs.

And during benchmarking, HiBench will monitor CPU, Network, Disk IO, Memory and system loads of all your slave nodes. The monitor report is also generated for each workload, each language APIs.

## Flexible configurations ##

HiBench4.0 improved configurations greatly. There're several built-in data scale profiles, compression profiles. You can switch different profiles very easily.

Sometimes, some workload needs slightly different parameter. Now, you can configure every parameters (including spark conf) in global, workload and language API scope, which is great for fine tuning.

## Others ##

  - 4-step quick start added to document.
  - Colorful log output, progress bar for MapReduce and Spark.
  - A strictly assertion which will detect error at earlier stage.

We have verified HiBench 4.0 with newest Hadoop distribution versions, including CDH5.3.2(MRv1, MRv2), Apache Hadoop 1.0.4, 1.2.1,  Apache Hadoop 2.2.0, 2.5.2, Apache Spark 1.2, 1.3.

## Contributors ##

The following developers contributed to this release (ordered by Github ID):

Daoyuan Wang(@adrian-wang)
Earnest(@Earne)
Minho Kim (@eoriented)
Jie Huang(@GraceH)
Joseph Lorenzini(@jaloren)
Jay Vyas(@jayunit100)
Jintao Guan(@jintaoguan)
Kai Wei(@kai-wei)
Qi Lv(@lvsoft)
Nishkam Ravi (@nishkamravi2)
(@pipamc)
Neo Qian(@qiansl127)
Mingfei Shi(@shimingfei)
Dong Li(@zkld123)

Thanks to everyone who contributed! We are looking forward to more contributions from every one for next release soon.
Downloads

 Source code (zip)
 Source code (tar.gz)
