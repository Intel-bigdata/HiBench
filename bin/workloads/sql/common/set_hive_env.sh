#!/usr/bin/env bash
# why replace guava jar ?
# When we used hadoop 3.2.* and 3.1.* version , Running hive-3.0.0 will fail due to guava is not compatible with hadoop version, need to update guava to 27.0.1.
# why create metadata schema
# when we used hadoop 3.2.* and 3.1.* and 3.0.* version, Running hive-3.0.0 will fail because unable to instantiate org.apache.hadoop,hive.ql.metadata.SessionHiveMetaStoreClient, we need to create metadata.

if [[ $HADOOP_HOME =~ "3.2" || $HADOOP_HOME =~ "3.1" ]];then
    echo " replace guava jar nad create metadata schema"
    # replace guava jar
    rm -rf $HIVE_HOME/lib/guava-19.0.jar
    cp ${HIVEBENCH_TEMPLATE}/lib/guava-27.0.1-jre.jar $HIVE_HOME/lib
    # create metadata schema
    rm -rf $HIVE_HOME/../metastore_db
    echo "$HIVE_HOME/bin/schematool -initSchema -dbType derby"
    $HIVE_HOME/bin/schematool -initSchema -dbType derby
elif [[ $HADOOP_HOME =~ "3.0" ]];then
    echo " create metadata schema"
    # create metadata schema
    rm -rf $HIVE_HOME/../metastore_db
    echo "$HIVE_HOME/bin/schematool -initSchema -dbType derby"
    $HIVE_HOME/bin/schematool -initSchema -dbType derby
fi
