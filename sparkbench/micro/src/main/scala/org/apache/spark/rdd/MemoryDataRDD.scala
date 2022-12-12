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

package org.apache.spark.rdd

import org.apache.spark.internal.Logging
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}

class MemoryDataRDD(
    sc : SparkContext,
    numPartitions: Int,
    numRecords: Int,
    recordSize: Int)
  extends RDD[Array[Byte]](sc, Nil) with Logging {

//  val localData = Range(0, recordSize).map(i => i.toByte).toArray

  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    val iter = new Iterator[Array[Byte]] {
      private val inputMetrics = context.taskMetrics().inputMetrics
      private val existingBytesRead = inputMetrics.bytesRead
      private var recordsRead = 0
      private var lastSumRecord = 0

      val localData = Range(0, recordSize).map(i => i.toByte).toArray

      context.addTaskCompletionListener[Unit] { context =>
        inputMetrics.setBytesRead(existingBytesRead + (recordsRead - lastSumRecord) * recordSize * 1L)
      }

      override def hasNext: Boolean = {
        recordsRead < numRecords
      }

      override def next(): Array[Byte] = {
        if (!hasNext) {
          throw new NoSuchElementException("End of stream")
        }
        inputMetrics.incRecordsRead(1)
        if (inputMetrics.recordsRead % 1000 == 0) {
          inputMetrics.setBytesRead(existingBytesRead + (recordsRead - lastSumRecord) * recordSize * 1L)
          lastSumRecord = recordsRead
        }
        recordsRead = recordsRead + 1
        localData
      }
    }
    new InterruptibleIterator(context, iter)
  }

  override protected def getPartitions: Array[Partition] = {
    val result = new Array[Partition](numPartitions)
    for (i <- 0 until numPartitions) {
      result(i) = new MemPartition(id, i)
    }
    result
  }

  private class MemPartition(
      rddId: Int,
      val index: Int)
    extends Partition {

    override def hashCode(): Int = 31 * (31 + rddId) + index
  }
}
