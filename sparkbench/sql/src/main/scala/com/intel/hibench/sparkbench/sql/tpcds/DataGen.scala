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

import com.databricks.spark.sql.perf.tpcds.Tables
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

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

    val sc = new SparkContext()
    val hiveContext = new HiveContext(sc)

    val tables = new Tables(hiveContext, dsdgenPath, tableSize)

    val tableNames = getRateMap().map(_._1).toList

    genDataWithTableFilters(
      tables, hdfs, "parquet", true, false, false, false, false, tableNames, tableSize)
    tables.createExternalTables(hdfs, "parquet", s"tpcds_${tableSize}g", true)
  }

  def genDataWithTableFilters(
                               tables: Tables,
                               location: String,
                               format: String,
                               overwrite: Boolean,
                               partitionTables: Boolean,
                               useDoubleForDecimal: Boolean,
                               clusterByPartitionColumns: Boolean,
                               filterOutNullPartitionValues: Boolean,
                               tableFilters: List[String],
                               tableSize: Int): Unit = {
    val rateMap = getRateMap()
    tableFilters.foreach(
      tableName => {
        var numPartitions = 1
        if(rateMap(tableName) != 1) {
          numPartitions = numPartitions.max(tableSize / 1000 * rateMap(tableName))
        }
        tables.genData(
          location, "parquet", overwrite, partitionTables, useDoubleForDecimal,
          clusterByPartitionColumns, filterOutNullPartitionValues, tableName, numPartitions)
      }
    )
  }

  def getRateMap(): Map[String, Int] = {
    Map[String, Int](
      "catalog_sales" -> 80,
      "catalog_returns" -> 10,
      "inventory" -> 5,
      "store_sales" -> 100,
      "store_returns" -> 10,
      "web_sales" -> 40,
      "web_returns" -> 5,
      "call_center" -> 1,
      "catalog_page" -> 1,
      "customer" -> 1,
      "customer_address" -> 1,
      "customer_demographics" -> 1,
      "date_dim" -> 1,
      "household_demographics" -> 1,
      "income_band" -> 1,
      "item" -> 1,
      "promotion" -> 1,
      "reason" -> 1,
      "ship_mode" -> 1,
      "store" -> 1,
      "time_dim" -> 1,
      "warehouse" -> 1,
      "web_page" -> 1,
      "web_site" -> 1
    )
  }
}
