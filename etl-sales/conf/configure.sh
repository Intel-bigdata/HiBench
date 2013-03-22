#!/bin/bash

# dbgen paths
DBGEN_HOME=/home/hadoop/projects/dbgen

# paths
DBGEN_BASE_HDFS=/HiBench/TPCDS
DBGEN_INPUT=${DBGEN_BASE_HDFS}/Input
DBGEN_OUTPUT=${DBGEN_BASE_HDFS}/Output
DBGEN_DATA=${DBGEN_BASE_HDFS}/DATA

# tables to create
TABLES_NO_PARALLEL="
dbgen_version
date_dim
time_dim
call_center
income_band
household_demographics
item
warehouse
promotion
reason
ship_mode
store
web_site
web_page
"
TABLES_PARALLEL="
customer
customer_address
customer_demographics
inventory
web_sales
web_returns
"

# parameters of dbgen
SCALE=100
PARALLEL=128
TIMEOUT=0
