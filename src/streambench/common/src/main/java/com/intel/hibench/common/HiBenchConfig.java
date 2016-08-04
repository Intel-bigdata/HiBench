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


package com.intel.hibench.common;

/**
 * common configurations used in HiBench project are defined here. Later we plan to move this file
 * to higher project.
 */
public class HiBenchConfig {

  // =====================================
  // Spark Related Conf
  // =====================================
  public static String SPARK_MASTER = "hibench.spark.master";

  // =====================================
  // DFS Related Conf
  // =====================================
  public static String DFS_MASTER = "hibench.hdfs.master";

  // =====================================
  // YARN Related Conf
  // =====================================
  public static String YARN_EXECUTOR_NUMBER = "hibench.yarn.executor.num";

  public static String YARN_EXECUTOR_CORES = "hibench.yarn.executor.cores";
}
