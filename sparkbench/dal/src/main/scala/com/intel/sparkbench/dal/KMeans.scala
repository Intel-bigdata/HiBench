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

//From DAAL https://github.com/intel/daal/blob/master/samples/scala/spark/sources/KMeans.scala
package daal_for_mllib

import org.apache.spark.api.java._
import org.apache.spark.api.java.function._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.clustering.KMeansModel

import org.apache.spark.SparkContext
import org.apache.spark.util.AccumulatorV2
import scala.collection.JavaConverters._

import scala.Tuple2
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.storage.StorageLevel

import com.intel.daal.algorithms.kmeans._
import com.intel.daal.algorithms.kmeans.init._
import com.intel.daal.data_management.data._
import com.intel.daal.services._

class DAALKMeansModel(override val clusterCenters: Array[Vector]) extends KMeansModel(clusterCenters) {}

object KMeans {
    def train(data: RDD[Vector], nClusters: Int, nIterations: Int): DAALKMeansModel = {
        new KMeans().setNClusters(nClusters).setNIterations(nIterations).run(data)
    }

    def train(data: RDD[Vector], nClusters: Int, nIterations: Int, dummy: Int, initializationMethod: String): DAALKMeansModel = {
        // initializationMethod = "random" is only available at the moment
        new KMeans().setNClusters(nClusters).setNIterations(nIterations).run(data)
    }
}

class KMeansResult private (var centroids: HomogenNumericTable, var nIterations: Int, var nClusters: Long) {}

class KMeans private (private var nClusters: Int, private var nIterations: Int) {

    def this() = this(10, 10)

    private var initialModel: Option[KMeansModel] = None

    def getNClusters: Int = nClusters

    def setNClusters(nClusters: Int): this.type = {
        this.nClusters = nClusters
        this
    }

    def setInitialModel(model: DAALKMeansModel): this.type = {
        require(model.k == nClusters, "mismatched cluster count")
        initialModel = Some(model)
        this
    }

    def getNIterations: Int = nIterations

    def setNIterations(nIterations: Int): this.type = {
        this.nIterations = nIterations
        this
    }

    def run(data: RDD[Vector]): DAALKMeansModel = {
        train(data, nClusters, nIterations)
    }

    def train(data: RDD[Vector], nClusters: Int, nIterations: Int) : DAALKMeansModel = {

        val internRDD = getAsPairRDD(data)
        val tupleRes = computeOffsets(internRDD)

        val offsets = tupleRes._1
        val numVectors = tupleRes._2

        var centroids = initialModel match {
            case Some(kMeansCenters) => getCentroids(kMeansCenters.clusterCenters)
            case None => computeInit(nClusters, numVectors, offsets, internRDD)
        }

        val it: Int = 0
        for(it <- 1 to nIterations) {
            centroids = computeIter(nClusters, centroids, internRDD)
        }
        internRDD.unpersist(true)

        val context = new DaalContext()
        centroids.unpack(context)
        val resTable = centroids.getDoubleArray()
        val resArray = new Array[Vector](nClusters)
        val numCols = centroids.getNumberOfColumns().asInstanceOf[Int]
        var i: Int = 0
        for (i <- 0 to (nClusters - 1)) {
            val internArray = new Array[Double](numCols)
            var j: Int = 0
            for (j <- 0 to (numCols - 1)) {
                internArray(j) = resTable(i * numCols + j)
            }
            resArray(i) = Vectors.dense(internArray)
        }
        centroids.dispose()

        new DAALKMeansModel(resArray)
    }

    def getCentroids(clusterCenters: Array[Vector]): HomogenNumericTable = {
        val nRows = clusterCenters.length
        val nCols = clusterCenters(0).toArray.length
        val arrData = new Array[Double](nRows * nCols)
        for (i <- 0 to (nRows - 1)) {
            clusterCenters(i).toArray.copyToArray(arrData, i * nCols)
        }
        val context = new DaalContext()
        val centroids = new HomogenNumericTable(context, arrData, nCols, nRows)
        centroids.pack()
        context.dispose()
        centroids
    }

    def getAsPairRDD(data: RDD[Vector]): RDD[HomogenNumericTable] = {

        val tablesRDD = data.mapPartitions(
            (it: Iterator[Vector]) => {

                val tables = new ArrayBuffer[HomogenNumericTable]()
                var arrays = new ArrayBuffer[Array[Double]]()
                it.foreach{curVector =>
                    val rowData = curVector.toArray
                    arrays += rowData
                }
                val numCols: Int = arrays(0).length
                val numRows: Int = arrays.length

                val arrData = new Array[Double](numRows * numCols)
                var i: Int = 0
                for (i <- 0 to (numRows - 1)) {
                    arrays(i).copyToArray(arrData, i * numCols)
                }
                val context = new DaalContext()
                val table = new HomogenNumericTable(context, arrData, numCols, numRows)
                table.pack()
                tables += table

                context.dispose()
                arrays.clear
                tables.iterator
            }
        ).persist(StorageLevel.MEMORY_AND_DISK)
        tablesRDD
    }

    def computeOffsets(internRDD: RDD[HomogenNumericTable]) : Tuple2[Array[Long], Long] = {
        val tmpOffsetRDD = internRDD.mapPartitions(
            (it: Iterator[HomogenNumericTable]) => {
                val table = it.next
                val context = new DaalContext()
                table.unpack(context)
                val numRows = table.getNumberOfRows()
                table.pack()
                context.dispose

                Iterator(numRows)
            })

        val numbersOfRows = tmpOffsetRDD.collect

        tmpOffsetRDD.unpersist(true)

        var numVectors: Long = 0

        val i: Int = 0
        for (i <- 0 to (numbersOfRows.size - 1)) {
            numVectors = numVectors + numbersOfRows(i)
        }

        val partition = new Array[Long](numbersOfRows.size + 1)
        partition(0) = 0
        for(i <- 0 to (numbersOfRows.size - 1)) {
            partition(i + 1) = partition(i) + numbersOfRows(i)
        }
        new Tuple2(partition, numVectors)
    }

    def computeCost(model: KMeansResult, data: RDD[Vector]) : Double = {

        val internRDD = getAsPairRDD(data)

        val partsRDDcompute = computeLocal(model.nClusters.asInstanceOf[Long], model.centroids, internRDD)

        val cost = computeMasterCost(model.nClusters.asInstanceOf[Long], partsRDDcompute)

        internRDD.unpersist(true)
        partsRDDcompute.unpersist(true)

        cost
    }

    def computeInit(nClusters: Int, nV: Long, offsets: Array[Long], internRDD: RDD[HomogenNumericTable]) : HomogenNumericTable = {
        val contextM = new DaalContext()
        /* Create an algorithm to compute k-means on the master node */
        val kmeansMasterInit = new InitDistributedStep2Master(contextM,
                                                              classOf[java.lang.Double],
                                                              InitMethod.randomDense,
                                                              nClusters.asInstanceOf[Long])
        val tmpInitRDD = internRDD.mapPartitionsWithIndex(
            (index, it: Iterator[HomogenNumericTable]) => {
                val table = it.next
                val context = new DaalContext()
                /* Create an algorithm to initialize the K-Means algorithm on local nodes */
                val kmeansLocalInit = new InitDistributedStep1Local(context,
                                                                    classOf[java.lang.Double],
                                                                    InitMethod.randomDense,
                                                                    nClusters,
                                                                    nV,
                                                                    offsets(index))
                /* Set the input data on local nodes */
                table.unpack(context)
                kmeansLocalInit.input.set(InitInputId.data, table)

                /* Initialize the K-Means algorithm on local nodes */
                val pres = kmeansLocalInit.compute()
                pres.pack()
                table.pack()
                context.dispose()

                Iterator(pres)
            })

        val partsList = tmpInitRDD.collect

        tmpInitRDD.unpersist(true)

        /* Add partial results computed on local nodes to the algorithm on the master node */
        val i: Int = 0
        for (i <- 0 to (partsList.size - 1)) {
            partsList(i).unpack(contextM)
            kmeansMasterInit.input.add(InitDistributedStep2MasterInputId.partialResults, partsList(i))
        }

        /* Compute k-means on the master node */
        kmeansMasterInit.compute()

        /* Finalize computations and retrieve the results */
        val initResult = kmeansMasterInit.finalizeCompute()
        val ret = initResult.get(InitResultId.centroids).asInstanceOf[HomogenNumericTable]
        ret.pack()

        contextM.dispose()

        ret
    }

    def computeLocal(nClusters: Long, centroids: HomogenNumericTable, internRDD: RDD[HomogenNumericTable]) : RDD[PartialResult] = {

        val partsRDDcompute = internRDD.mapPartitions(
            (it: Iterator[HomogenNumericTable]) => {
                val table = it.next
                val context = new DaalContext()

                /* Create an algorithm to compute k-means on local nodes */
                val kmeansLocal = new DistributedStep1Local(context, classOf[java.lang.Double], Method.defaultDense, nClusters)
                kmeansLocal.parameter.setAssignFlag(false)

                /* Set the input data on local nodes */
                table.unpack(context)
                centroids.unpack(context)
                kmeansLocal.input.set(InputId.data, table)
                kmeansLocal.input.set(InputId.inputCentroids, centroids)

                /* Compute k-means on local nodes */
                val pres = kmeansLocal.compute()
                pres.pack()

                table.pack()
                centroids.pack()
                context.dispose()

                Iterator(pres)
            }).persist(StorageLevel.MEMORY_AND_DISK)

        partsRDDcompute
    }

    def computeMaster(nClusters: Long, partsRDDcompute: RDD[Tuple2[Int, PartialResult]]) : HomogenNumericTable = {
        val context = new DaalContext()

        /* Create an algorithm to compute k-means on the master node */
        val kmeansMaster = new DistributedStep2Master(context, classOf[java.lang.Double], Method.defaultDense, nClusters)

        val partsList = partsRDDcompute.collect()

        partsRDDcompute.unpersist(true)

        /* Add partial results computed on local nodes to the algorithm on the master node */
        val i: Int = 0
        for (i <- 0 to (partsList.size - 1)) {
            partsList(i)._2.unpack(context)
            kmeansMaster.input.add(DistributedStep2MasterInputId.partialResults, partsList(i)._2)
        }

        /* Compute k-means on the master node */
        kmeansMaster.compute()

        /* Finalize computations and retrieve the results */
        val res = kmeansMaster.finalizeCompute()

        val centroids = res.get(ResultId.centroids).asInstanceOf[HomogenNumericTable]
        centroids.pack()

        context.dispose()

        centroids
    }

    def computeIter(nClusters: Long, centroids: HomogenNumericTable, internRDD: RDD[HomogenNumericTable]) : HomogenNumericTable = {

        val contextI = new DaalContext()
        /* Create an algorithm to compute k-means on the master node */
        val kmeansMaster = new DistributedStep2Master(contextI, classOf[java.lang.Double], Method.defaultDense, nClusters)

        val tmpIterRDD = internRDD.mapPartitions(
            (it: Iterator[HomogenNumericTable]) => {
                val table = it.next
                val context = new DaalContext()
                val kmeansLocal = new DistributedStep1Local(context, classOf[java.lang.Double], Method.defaultDense, nClusters)
                kmeansLocal.parameter.setAssignFlag(false)
                table.unpack(context)
                centroids.unpack(context)
                kmeansLocal.input.set(InputId.data, table)
                kmeansLocal.input.set(InputId.inputCentroids, centroids)
                val pres = kmeansLocal.compute()
                pres.pack()
                table.pack()
                centroids.pack()
                context.dispose()

                Iterator(pres)
            })

        val reducedPartialResult = tmpIterRDD.treeReduce(
            (pr1, pr2) => {
                val context = new DaalContext()

                val kmeansMaster = new DistributedStep2Master(context, classOf[java.lang.Double], Method.defaultDense, nClusters)

                pr1.unpack(context)
                pr2.unpack(context)
                kmeansMaster.input.add(DistributedStep2MasterInputId.partialResults, pr1)
                kmeansMaster.input.add(DistributedStep2MasterInputId.partialResults, pr2)

                val pr = kmeansMaster.compute()

                pr.pack()
                context.dispose()

                pr
            }, 3)

        tmpIterRDD.unpersist(true)

        reducedPartialResult.unpack(contextI)
        kmeansMaster.input.add(DistributedStep2MasterInputId.partialResults, reducedPartialResult)
        /* Compute k-means on the master node */

        kmeansMaster.compute()

        /* Finalize computations and retrieve the results */
        val res = kmeansMaster.finalizeCompute()

        val centroidsI = res.get(ResultId.centroids).asInstanceOf[HomogenNumericTable]
        centroidsI.pack()

        contextI.dispose()

        centroidsI
    }

    def computeMasterCost(nClusters: Long, partsRDDcompute: RDD[PartialResult]) : Double = {
        val context = new DaalContext()
        /* Create an algorithm to compute k-means on the master node */
        val kmeansMaster = new DistributedStep2Master(context, classOf[java.lang.Double], Method.defaultDense, nClusters)

        val partsList = partsRDDcompute.collect()

        /* Add partial results computed on local nodes to the algorithm on the master node */
        val i: Int = 0
        for (i <- 0 to (partsList.size - 1)) {
            partsList(i).unpack(context)
            kmeansMaster.input.add(DistributedStep2MasterInputId.partialResults, partsList(i))
        }

        /* Compute k-means on the master node */
        kmeansMaster.compute()

        /* Finalize computations and retrieve the results */
        val res = kmeansMaster.finalizeCompute()

        val goalFunc = res.get(ResultId.objectiveFunction).asInstanceOf[HomogenNumericTable]

        val goal = goalFunc.getDoubleArray()
        val cost = goal(0)

        context.dispose()

        cost
    }
}
