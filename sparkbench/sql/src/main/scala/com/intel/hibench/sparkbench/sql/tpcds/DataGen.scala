/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.hibench.sparkbench.sql.tpcds

import org.apache.spark.sql.SparkSession

object DataGen {

  def main(args: Array[String]): Unit = {

    if (args.length != 3){
      System.err.println(
        s"Usage: $DataGen <INPUT_HDFS> <TABLESIZE>(/G) <DSDGEN_PATH>"
      )
      System.exit(1)
    }

    val hdfs = args(0)
    val tableSize = args(1).toInt
    val dsdgenPath = args(2)

    val spark = SparkSession
      .builder()
      .appName("TPC-DS Gendata")
      .enableHiveSupport()
      .getOrCreate()

    val tables = new Tables(spark.sqlContext, dsdgenPath, tableSize)

    tables.genData(hdfs, "parquet", true, false, false, false, false, "catalog_sales", 1)
    tables.genData(hdfs, "parquet", true, false, false, false, false, "catalog_returns", 1)
    tables.genData(hdfs, "parquet", true, false, false, false, false, "inventory", 1)
    tables.genData(hdfs, "parquet", true, false, false, false, false, "store_sales", 1)
    tables.genData(hdfs, "parquet", true, false, false, false, false, "store_returns", 1)
    tables.genData(hdfs, "parquet", true, false, false, false, false, "web_sales", 1)
    tables.genData(hdfs, "parquet", true, false, false, false, false, "web_returns", 1)
    tables.genData(hdfs, "parquet", true, false, false, false, false, "call_center", 1)
    tables.genData(hdfs, "parquet", true, false, false, false, false, "catalog_page", 1)
    tables.genData(hdfs, "parquet", true, false, false, false, false, "customer", 1)
    tables.genData(hdfs, "parquet", true, false, false, false, false, "customer_address", 1)
    tables.genData(hdfs, "parquet", true, false, false, false, false, "customer_demographics", 1)
    tables.genData(hdfs, "parquet", true, false, false, false, false, "date_dim", 1)
    tables.genData(hdfs, "parquet", true, false, false, false, false, "household_demographics", 1)
    tables.genData(hdfs, "parquet", true, false, false, false, false, "income_band", 1)
    tables.genData(hdfs, "parquet", true, false, false, false, false, "item", 1)
    tables.genData(hdfs, "parquet", true, false, false, false, false, "promotion", 1)
    tables.genData(hdfs, "parquet", true, false, false, false, false, "reason", 1)
    tables.genData(hdfs, "parquet", true, false, false, false, false, "ship_mode", 1)
    tables.genData(hdfs, "parquet", true, false, false, false, false, "store", 1)
    tables.genData(hdfs, "parquet", true, false, false, false, false, "time_dim", 1)
    tables.genData(hdfs, "parquet", true, false, false, false, false, "warehouse", 1)
    tables.genData(hdfs, "parquet", true, false, false, false, false, "web_page", 1)
    tables.genData(hdfs, "parquet", true, false, false, false, false, "web_site", 1)

    tables.createExternalTables(hdfs, "parquet", s"tpcds_${tableSize}g", true)
  }

}
