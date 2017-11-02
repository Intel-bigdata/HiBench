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

import java.io.File

import scala.sys.process._
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.joda.time.{DateTime, DateTimeZone}

class Tables(sqlContext: SQLContext, dsdgenDir: String, scaleFactor: Int) extends Serializable {
  import sqlContext.implicits._

  private val log = LoggerFactory.getLogger(getClass)

  def sparkContext = sqlContext.sparkContext
  val dsdgenLocalDir = "/tmp/tools"
  val dsdgenLocalPath = s"${dsdgenLocalDir}/dsdgen"

  def fetchDsdgenFromHDFS(hadoopExecutable: String) = {
    try {
      log.info(s"Executing: ${hadoopExecutable} fs -get ${dsdgenDir} ${dsdgenLocalDir}")
      s"${hadoopExecutable} fs -get ${dsdgenDir} ${dsdgenLocalDir}".!!
    } catch {
      case e: java.lang.RuntimeException => log.info(e.toString)
    }
  }

  def changeDsdgenPermission() = {
    try {
      log.info(s"Executing: chmod +x ${dsdgenLocalPath} ${dsdgenLocalDir}/distcomp ${dsdgenLocalDir}/mkheader")
      s"chmod +x ${dsdgenLocalPath} ${dsdgenLocalDir}/distcomp ${dsdgenLocalDir}/mkheader".!!
    } catch {
      case e: java.lang.RuntimeException => log.info(e.toString)
    }
  }

  case class Table(name: String, fields: StructField*) {
    val schema = StructType(fields)

    /**
      *  If convertToSchema is true, the data from generator will be parsed into columns and
      *  converted to `schema`. Otherwise, it just outputs the raw data (as a single STRING column).
      */
    def generateAndConvertToDF(hadoop_executable: String, convertToSchema: Boolean, numPartition: Int) = {

      val generatedData = {
        sparkContext.parallelize(1 to numPartition, numPartition).flatMap { i =>
          if (! new File(dsdgenLocalPath).exists)
            fetchDsdgenFromHDFS(hadoop_executable)
          changeDsdgenPermission()
          val localToolsDir = if (new java.io.File(dsdgenLocalPath).exists) {
            s"${dsdgenLocalDir}"
          } else {
            sys.error(s"Could not find dsdgen at ${dsdgenLocalPath} or /${dsdgenLocalPath}. Run install")
          }

          val rngseed = DateTime.now(DateTimeZone.UTC).getMillis() % 997
          val parallel = if (numPartition > 1) s"-parallel $numPartition -child $i" else ""
          val commands = Seq(
            "bash", "-c",
            s"cd ${dsdgenLocalDir} && ./dsdgen -table $name -filter Y -scale $scaleFactor -RNGSEED $rngseed $parallel")
          commands.lines
        }
      }

      generatedData.setName(s"$name, sf=$scaleFactor, strings")

      val rows = generatedData.mapPartitions { iter =>
        iter.map { l =>
          if (convertToSchema) {
            val values = l.split("\\|", -1).dropRight(1).map { v =>
              if (v.equals("")) {
                // If the string value is an empty string, we turn it to a null
                null
              } else {
                v
              }
            }
            Row.fromSeq(values)
          } else {
            Row.fromSeq(Seq(l))
          }
        }
      }

      if (convertToSchema) {
        val stringData =
          sqlContext.createDataFrame(
            rows,
            StructType(schema.fields.map(f => StructField(f.name, StringType))))

        val convertedData = {
          val columns = schema.fields.map { f =>
            col(f.name).cast(f.dataType).as(f.name)
          }
          stringData.select(columns: _*)
        }

        convertedData
      } else {
        sqlContext.createDataFrame(rows, StructType(Seq(StructField("value", StringType))))
      }
    }

    def useDoubleForDecimal(): Table = {
      val newFields = fields.map { field =>
        val newDataType = field.dataType match {
          case decimal: DecimalType => DoubleType
          case other => other
        }
        field.copy(dataType = newDataType)
      }

      Table(name, newFields:_*)
    }

    def genData(
        location: String,
        format: String,
        overwrite: Boolean,
        numPartitions: Int): Unit = {
      val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Ignore

      val data = generateAndConvertToDF(DataGen.HADOOP_EXECUTABLE, format != "text", numPartitions)

      val writer = data.write
      writer.format(format).mode(mode)
      println(s"Generating table $name in database to $location with save mode $mode.")
      log.info(s"Generating table $name in database to $location with save mode $mode.")
      writer.save(location)
    }

    def createExternalTable(location: String, format: String, databaseName: String, overwrite: Boolean): Unit = {
      val qualifiedTableName = databaseName + "." + name
      val tableExists = sqlContext.tableNames(databaseName).contains(name)
      if (overwrite) {
        sqlContext.sql(s"DROP TABLE IF EXISTS $databaseName.$name")
      }
      if (!tableExists || overwrite) {
        println(s"Creating external table $name in database $databaseName using data stored in $location.")
        log.info(s"Creating external table $name in database $databaseName using data stored in $location.")
        sqlContext.createExternalTable(qualifiedTableName, location, format)
      }
    }

  }

  def genData(
      location: String,
      format: String,
      overwrite: Boolean,
      useDoubleForDecimal: Boolean,
      tableFilter: String = "",
      numPartitions: Int = 1): Unit = {

    val tableToGenerate = tables.filter(_.name == tableFilter)
    if (tableToGenerate.isEmpty) {
      throw new RuntimeException("Bad table name filter: " + tableFilter)
    }

    val withSpecifiedDataType = if (useDoubleForDecimal) {
      tableToGenerate.map(_.useDoubleForDecimal())
    } else {
      tableToGenerate
    }

    withSpecifiedDataType.foreach { table =>
      val tableLocation = s"$location/${table.name}"
      table.genData(tableLocation, format, overwrite, numPartitions)
    }
  }

  def createExternalTable(
      location: String,
      format: String,
      databaseName: String,
      overwrite: Boolean,
      tableFilter: String = ""): Unit = {

    val tableToCreateExternalTable = tables.filter(_.name == tableFilter)
    if (tableToCreateExternalTable.isEmpty) {
      throw new RuntimeException("Bad table name filter: " + tableFilter)
    }

    sqlContext.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")
    tableToCreateExternalTable.foreach { table =>
      val tableLocation = s"$location/${table.name}"
      table.createExternalTable(tableLocation, format, databaseName, overwrite)
    }
    sqlContext.sql(s"USE $databaseName")
    println(s"The current database has been set to $databaseName.")
    log.info(s"The current database has been set to $databaseName.")
  }

  val tables = Seq(
    Table("catalog_sales",
      'cs_sold_date_sk          .int,
      'cs_sold_time_sk          .int,
      'cs_ship_date_sk          .int,
      'cs_bill_customer_sk      .int,
      'cs_bill_cdemo_sk         .int,
      'cs_bill_hdemo_sk         .int,
      'cs_bill_addr_sk          .int,
      'cs_ship_customer_sk      .int,
      'cs_ship_cdemo_sk         .int,
      'cs_ship_hdemo_sk         .int,
      'cs_ship_addr_sk          .int,
      'cs_call_center_sk        .int,
      'cs_catalog_page_sk       .int,
      'cs_ship_mode_sk          .int,
      'cs_warehouse_sk          .int,
      'cs_item_sk               .int,
      'cs_promo_sk              .int,
      'cs_order_number          .int,
      'cs_quantity              .int,
      'cs_wholesale_cost        .decimal(7,2),
      'cs_list_price            .decimal(7,2),
      'cs_sales_price           .decimal(7,2),
      'cs_ext_discount_amt      .decimal(7,2),
      'cs_ext_sales_price       .decimal(7,2),
      'cs_ext_wholesale_cost    .decimal(7,2),
      'cs_ext_list_price        .decimal(7,2),
      'cs_ext_tax               .decimal(7,2),
      'cs_coupon_amt            .decimal(7,2),
      'cs_ext_ship_cost         .decimal(7,2),
      'cs_net_paid              .decimal(7,2),
      'cs_net_paid_inc_tax      .decimal(7,2),
      'cs_net_paid_inc_ship     .decimal(7,2),
      'cs_net_paid_inc_ship_tax .decimal(7,2),
      'cs_net_profit            .decimal(7,2)),
    Table("catalog_returns",
      'cr_returned_date_sk      .int,
      'cr_returned_time_sk      .int,
      'cr_item_sk               .int,
      'cr_refunded_customer_sk  .int,
      'cr_refunded_cdemo_sk     .int,
      'cr_refunded_hdemo_sk     .int,
      'cr_refunded_addr_sk      .int,
      'cr_returning_customer_sk .int,
      'cr_returning_cdemo_sk    .int,
      'cr_returning_hdemo_sk    .int,
      'cr_returning_addr_sk     .int,
      'cr_call_center_sk        .int,
      'cr_catalog_page_sk       .int,
      'cr_ship_mode_sk          .int,
      'cr_warehouse_sk          .int,
      'cr_reason_sk             .int,
      'cr_order_number          .int,
      'cr_return_quantity       .int,
      'cr_return_amount         .decimal(7,2),
      'cr_return_tax            .decimal(7,2),
      'cr_return_amt_inc_tax    .decimal(7,2),
      'cr_fee                   .decimal(7,2),
      'cr_return_ship_cost      .decimal(7,2),
      'cr_refunded_cash         .decimal(7,2),
      'cr_reversed_charge       .decimal(7,2),
      'cr_store_credit          .decimal(7,2),
      'cr_net_loss              .decimal(7,2)),
    Table("inventory",
      'inv_date_sk          .int,
      'inv_item_sk          .int,
      'inv_warehouse_sk     .int,
      'inv_quantity_on_hand .int),
    Table("store_sales",
      'ss_sold_date_sk      .int,
      'ss_sold_time_sk      .int,
      'ss_item_sk           .int,
      'ss_customer_sk       .int,
      'ss_cdemo_sk          .int,
      'ss_hdemo_sk          .int,
      'ss_addr_sk           .int,
      'ss_store_sk          .int,
      'ss_promo_sk          .int,
      'ss_ticket_number     .int,
      'ss_quantity          .int,
      'ss_wholesale_cost    .decimal(7,2),
      'ss_list_price        .decimal(7,2),
      'ss_sales_price       .decimal(7,2),
      'ss_ext_discount_amt  .decimal(7,2),
      'ss_ext_sales_price   .decimal(7,2),
      'ss_ext_wholesale_cost.decimal(7,2),
      'ss_ext_list_price    .decimal(7,2),
      'ss_ext_tax           .decimal(7,2),
      'ss_coupon_amt        .decimal(7,2),
      'ss_net_paid          .decimal(7,2),
      'ss_net_paid_inc_tax  .decimal(7,2),
      'ss_net_profit        .decimal(7,2)),
    Table("store_returns",
      'sr_returned_date_sk  .long,
      'sr_return_time_sk    .long,
      'sr_item_sk           .long,
      'sr_customer_sk       .long,
      'sr_cdemo_sk          .long,
      'sr_hdemo_sk          .long,
      'sr_addr_sk           .long,
      'sr_store_sk          .long,
      'sr_reason_sk         .long,
      'sr_ticket_number     .long,
      'sr_return_quantity   .long,
      'sr_return_amt        .decimal(7,2),
      'sr_return_tax        .decimal(7,2),
      'sr_return_amt_inc_tax.decimal(7,2),
      'sr_fee               .decimal(7,2),
      'sr_return_ship_cost  .decimal(7,2),
      'sr_refunded_cash     .decimal(7,2),
      'sr_reversed_charge   .decimal(7,2),
      'sr_store_credit      .decimal(7,2),
      'sr_net_loss          .decimal(7,2)),
    Table("web_sales",
      'ws_sold_date_sk          .int,
      'ws_sold_time_sk          .int,
      'ws_ship_date_sk          .int,
      'ws_item_sk               .int,
      'ws_bill_customer_sk      .int,
      'ws_bill_cdemo_sk         .int,
      'ws_bill_hdemo_sk         .int,
      'ws_bill_addr_sk          .int,
      'ws_ship_customer_sk      .int,
      'ws_ship_cdemo_sk         .int,
      'ws_ship_hdemo_sk         .int,
      'ws_ship_addr_sk          .int,
      'ws_web_page_sk           .int,
      'ws_web_site_sk           .int,
      'ws_ship_mode_sk          .int,
      'ws_warehouse_sk          .int,
      'ws_promo_sk              .int,
      'ws_order_number          .int,
      'ws_quantity              .int,
      'ws_wholesale_cost        .decimal(7,2),
      'ws_list_price            .decimal(7,2),
      'ws_sales_price           .decimal(7,2),
      'ws_ext_discount_amt      .decimal(7,2),
      'ws_ext_sales_price       .decimal(7,2),
      'ws_ext_wholesale_cost    .decimal(7,2),
      'ws_ext_list_price        .decimal(7,2),
      'ws_ext_tax               .decimal(7,2),
      'ws_coupon_amt            .decimal(7,2),
      'ws_ext_ship_cost         .decimal(7,2),
      'ws_net_paid              .decimal(7,2),
      'ws_net_paid_inc_tax      .decimal(7,2),
      'ws_net_paid_inc_ship     .decimal(7,2),
      'ws_net_paid_inc_ship_tax .decimal(7,2),
      'ws_net_profit            .decimal(7,2)),
    Table("web_returns",
      'wr_returned_date_sk      .long,
      'wr_returned_time_sk      .long,
      'wr_item_sk               .long,
      'wr_refunded_customer_sk  .long,
      'wr_refunded_cdemo_sk     .long,
      'wr_refunded_hdemo_sk     .long,
      'wr_refunded_addr_sk      .long,
      'wr_returning_customer_sk .long,
      'wr_returning_cdemo_sk    .long,
      'wr_returning_hdemo_sk    .long,
      'wr_returning_addr_sk     .long,
      'wr_web_page_sk           .long,
      'wr_reason_sk             .long,
      'wr_order_number          .long,
      'wr_return_quantity       .long,
      'wr_return_amt            .decimal(7,2),
      'wr_return_tax            .decimal(7,2),
      'wr_return_amt_inc_tax    .decimal(7,2),
      'wr_fee                   .decimal(7,2),
      'wr_return_ship_cost      .decimal(7,2),
      'wr_refunded_cash         .decimal(7,2),
      'wr_reversed_charge       .decimal(7,2),
      'wr_account_credit        .decimal(7,2),
      'wr_net_loss              .decimal(7,2)),
    Table("call_center",
      'cc_call_center_sk        .int,
      'cc_call_center_id        .string,
      'cc_rec_start_date        .date,
      'cc_rec_end_date          .date,
      'cc_closed_date_sk        .int,
      'cc_open_date_sk          .int,
      'cc_name                  .string,
      'cc_class                 .string,
      'cc_employees             .int,
      'cc_sq_ft                 .int,
      'cc_hours                 .string,
      'cc_manager               .string,
      'cc_mkt_id                .int,
      'cc_mkt_class             .string,
      'cc_mkt_desc              .string,
      'cc_market_manager        .string,
      'cc_division              .int,
      'cc_division_name         .string,
      'cc_company               .int,
      'cc_company_name          .string,
      'cc_street_number         .string,
      'cc_street_name           .string,
      'cc_street_type           .string,
      'cc_suite_number          .string,
      'cc_city                  .string,
      'cc_county                .string,
      'cc_state                 .string,
      'cc_zip                   .string,
      'cc_country               .string,
      'cc_gmt_offset            .decimal(5,2),
      'cc_tax_percentage        .decimal(5,2)),
    Table("catalog_page",
      'cp_catalog_page_sk       .int,
      'cp_catalog_page_id       .string,
      'cp_start_date_sk         .int,
      'cp_end_date_sk           .int,
      'cp_department            .string,
      'cp_catalog_number        .int,
      'cp_catalog_page_number   .int,
      'cp_description           .string,
      'cp_type                  .string),
    Table("customer",
      'c_customer_sk             .int,
      'c_customer_id             .string,
      'c_current_cdemo_sk        .int,
      'c_current_hdemo_sk        .int,
      'c_current_addr_sk         .int,
      'c_first_shipto_date_sk    .int,
      'c_first_sales_date_sk     .int,
      'c_salutation              .string,
      'c_first_name              .string,
      'c_last_name               .string,
      'c_preferred_cust_flag     .string,
      'c_birth_day               .int,
      'c_birth_month             .int,
      'c_birth_year              .int,
      'c_birth_country           .string,
      'c_login                   .string,
      'c_email_address           .string,
      'c_last_review_date        .string),
    Table("customer_address",
      'ca_address_sk             .int,
      'ca_address_id             .string,
      'ca_street_number          .string,
      'ca_street_name            .string,
      'ca_street_type            .string,
      'ca_suite_number           .string,
      'ca_city                   .string,
      'ca_county                 .string,
      'ca_state                  .string,
      'ca_zip                    .string,
      'ca_country                .string,
      'ca_gmt_offset             .decimal(5,2),
      'ca_location_type          .string),
    Table("customer_demographics",
      'cd_demo_sk                .int,
      'cd_gender                 .string,
      'cd_marital_status         .string,
      'cd_education_status       .string,
      'cd_purchase_estimate      .int,
      'cd_credit_rating          .string,
      'cd_dep_count              .int,
      'cd_dep_employed_count     .int,
      'cd_dep_college_count      .int),
    Table("date_dim",
      'd_date_sk                 .int,
      'd_date_id                 .string,
      'd_date                    .string,
      'd_month_seq               .int,
      'd_week_seq                .int,
      'd_quarter_seq             .int,
      'd_year                    .int,
      'd_dow                     .int,
      'd_moy                     .int,
      'd_dom                     .int,
      'd_qoy                     .int,
      'd_fy_year                 .int,
      'd_fy_quarter_seq          .int,
      'd_fy_week_seq             .int,
      'd_day_name                .string,
      'd_quarter_name            .string,
      'd_holiday                 .string,
      'd_weekend                 .string,
      'd_following_holiday       .string,
      'd_first_dom               .int,
      'd_last_dom                .int,
      'd_same_day_ly             .int,
      'd_same_day_lq             .int,
      'd_current_day             .string,
      'd_current_week            .string,
      'd_current_month           .string,
      'd_current_quarter         .string,
      'd_current_year            .string),
    Table("household_demographics",
      'hd_demo_sk                .int,
      'hd_income_band_sk         .int,
      'hd_buy_potential          .string,
      'hd_dep_count              .int,
      'hd_vehicle_count          .int),
    Table("income_band",
      'ib_income_band_sk         .int,
      'ib_lower_bound            .int,
      'ib_upper_bound            .int),
    Table("item",
      'i_item_sk                 .int,
      'i_item_id                 .string,
      'i_rec_start_date          .string,
      'i_rec_end_date            .string,
      'i_item_desc               .string,
      'i_current_price           .decimal(7,2),
      'i_wholesale_cost          .decimal(7,2),
      'i_brand_id                .int,
      'i_brand                   .string,
      'i_class_id                .int,
      'i_class                   .string,
      'i_category_id             .int,
      'i_category                .string,
      'i_manufact_id             .int,
      'i_manufact                .string,
      'i_size                    .string,
      'i_formulation             .string,
      'i_color                   .string,
      'i_units                   .string,
      'i_container               .string,
      'i_manager_id              .int,
      'i_product_name            .string),
    Table("promotion",
      'p_promo_sk                .int,
      'p_promo_id                .string,
      'p_start_date_sk           .int,
      'p_end_date_sk             .int,
      'p_item_sk                 .int,
      'p_cost                    .decimal(15,2),
      'p_response_target         .int,
      'p_promo_name              .string,
      'p_channel_dmail           .string,
      'p_channel_email           .string,
      'p_channel_catalog         .string,
      'p_channel_tv              .string,
      'p_channel_radio           .string,
      'p_channel_press           .string,
      'p_channel_event           .string,
      'p_channel_demo            .string,
      'p_channel_details         .string,
      'p_purpose                 .string,
      'p_discount_active         .string),
    Table("reason",
      'r_reason_sk               .int,
      'r_reason_id               .string,
      'r_reason_desc             .string),
    Table("ship_mode",
      'sm_ship_mode_sk           .int,
      'sm_ship_mode_id           .string,
      'sm_type                   .string,
      'sm_code                   .string,
      'sm_carrier                .string,
      'sm_contract               .string),
    Table("store",
      's_store_sk                .int,
      's_store_id                .string,
      's_rec_start_date          .string,
      's_rec_end_date            .string,
      's_closed_date_sk          .int,
      's_store_name              .string,
      's_number_employees        .int,
      's_floor_space             .int,
      's_hours                   .string,
      's_manager                 .string,
      's_market_id               .int,
      's_geography_class         .string,
      's_market_desc             .string,
      's_market_manager          .string,
      's_division_id             .int,
      's_division_name           .string,
      's_company_id              .int,
      's_company_name            .string,
      's_street_number           .string,
      's_street_name             .string,
      's_street_type             .string,
      's_suite_number            .string,
      's_city                    .string,
      's_county                  .string,
      's_state                   .string,
      's_zip                     .string,
      's_country                 .string,
      's_gmt_offset              .decimal(5,2),
      's_tax_precentage          .decimal(5,2)),
    Table("time_dim",
      't_time_sk                 .int,
      't_time_id                 .string,
      't_time                    .int,
      't_hour                    .int,
      't_minute                  .int,
      't_second                  .int,
      't_am_pm                   .string,
      't_shift                   .string,
      't_sub_shift               .string,
      't_meal_time               .string),
    Table("warehouse",
      'w_warehouse_sk           .int,
      'w_warehouse_id           .string,
      'w_warehouse_name         .string,
      'w_warehouse_sq_ft        .int,
      'w_street_number          .string,
      'w_street_name            .string,
      'w_street_type            .string,
      'w_suite_number           .string,
      'w_city                   .string,
      'w_county                 .string,
      'w_state                  .string,
      'w_zip                    .string,
      'w_country                .string,
      'w_gmt_offset             .decimal(5,2)),
    Table("web_page",
      'wp_web_page_sk           .int,
      'wp_web_page_id           .string,
      'wp_rec_start_date        .date,
      'wp_rec_end_date          .date,
      'wp_creation_date_sk      .int,
      'wp_access_date_sk        .int,
      'wp_autogen_flag          .string,
      'wp_customer_sk           .int,
      'wp_url                   .string,
      'wp_type                  .string,
      'wp_char_count            .int,
      'wp_link_count            .int,
      'wp_image_count           .int,
      'wp_max_ad_count          .int),
    Table("web_site",
      'web_site_sk              .int,
      'web_site_id              .string,
      'web_rec_start_date       .date,
      'web_rec_end_date         .date,
      'web_name                 .string,
      'web_open_date_sk         .int,
      'web_close_date_sk        .int,
      'web_class                .string,
      'web_manager              .string,
      'web_mkt_id               .int,
      'web_mkt_class            .string,
      'web_mkt_desc             .string,
      'web_market_manager       .string,
      'web_company_id           .int,
      'web_company_name         .string,
      'web_street_number        .string,
      'web_street_name          .string,
      'web_street_type          .string,
      'web_suite_number         .string,
      'web_city                 .string,
      'web_county               .string,
      'web_state                .string,
      'web_zip                  .string,
      'web_country              .string,
      'web_gmt_offset           .string,
      'web_tax_percentage       .decimal(5,2))
  )
}
