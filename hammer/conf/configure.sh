#!/bin/bash

#paths
DSGEN_HOME=/home/hadoop/projects/dbgen/
HAMMER_HDFS_BASE=${DATA_HDFS}/hammer

# parameters for dsgen
SCALE=100
PARALLEL=128
REFRESHES=1 

# parameters for weblogs
PARTNUM=1

# date split
TRAINSTARTDATE=1998-01-02
TRAINENDDATE=2002-12-22
TESTSTARTDATE=2002-12-23
TESTENDDATE=2002-12-29
