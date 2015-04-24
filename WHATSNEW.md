# What's New in HiBench 4.0 #

1. Workloads:

    1. All workloads moved to workloads folder
    2. In each workload, there's different language API implementations: mapreduce, spark/java, spark/python, spark/scala. (Except for NutchIndexing, which don't have spark implementation)
    
2. Configurations:

    1. Environment variable based configuration is deprecated.
    2. Global config is placed under conf folder, with priority
    ordered by the leading number in filename (larger will override
    smaller).
    3. Configuration values can be nested.
    4. Configuration in each workload will override global config.
    5. Configuration in each Language APIs will over workload config.
    
3. Report:

    1. A report will be generated for each workload, each language
    API.
    2. An all-in-one config file will be generated in report folder,
    with hints about where/how the config values come from.
    3. A build-in system monitor will be generated with CPU, disk,
    network, memory, process numbers report for each workload.
    
4. Others:

    1. Most system configurations will be probed and inferred
    automatically, like Hadoop/Spark version/release, hadoop conf dir,
    master/slave nodes, depended jars, ...
    2. Driver log has been filtered and highlighted for better human
    readability.
    3. A more strictly build-in assertion mechanism will detect error
    at earlier time.
    