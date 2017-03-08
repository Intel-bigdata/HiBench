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

package com.intel.hibench.sparkbench.ml

import java.io._

import com.intel.hibench.sparkbench.common.IOCommon
import org.apache.spark.HashPartitioner
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.random._
import org.apache.spark.mllib.tree.configuration.{Algo, FeatureType}
import org.apache.spark.mllib.tree.model.{Split, DecisionTreeModel, Node, Predict}
import org.apache.spark.rdd.{PairRDDFunctions, RDD}

import scala.collection.JavaConverters._
import scala.util.Random
import scala.util.control.Breaks._


object LabeledPointDataGenerator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ML-DataGeneration")
    val sc = new SparkContext(conf)

    var outputPath = ""
    var numExamples: Long = 200000
    var numFeatures: Int = 20
    var labelType: Int = 2
    var fracCategoricalFeatures: Double = 1
    var fracBinaryFeatures: Double = 1
    var treeDepth: Int = 1
    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)
    val numPartitions = IOCommon.getProperty("hibench.default.shuffle.parallelism")
      .getOrElse((parallel / 2).toString).toInt

    if (args.length == 7) {
      outputPath = args(0)
      numExamples = args(1).toLong
      numFeatures = args(2).toInt
      labelType = args(3).toInt
      fracCategoricalFeatures = args(4).toDouble
      fracBinaryFeatures = args(5).toDouble
      treeDepth = args(6).toInt

      println(s"Output Path: $outputPath")
      println(s"Num of Examples: $numExamples")
      println(s"Num of Features: $numFeatures")
      println(s"Label Type: $labelType")
      println(s"Frac Categorical Features: $fracCategoricalFeatures")
      println(s"Frac Binary Features: $fracBinaryFeatures")
      println(s"Tree Depth: $outputPath")
    } else {
      System.err.println(
        s"Usage: $LabeledPointDataGenerator <OUTPUT_PATH> <NUM_EXAMPLES> <NUM_FEATURES> <LABELTYPE> <FRACCATEGORICALFEATURES> <FRACBINARYFEATURES> <TREEDEPTH>"
      )
      System.exit(1)
    }

    val data = generateDecisionTreeLabeledPoints(sc, math.ceil(numExamples * 1.25).toLong,
        numFeatures, numPartitions, labelType,
        fracCategoricalFeatures, fracBinaryFeatures, treeDepth)   
 
    data.saveAsTextFile(outputPath)
 
    sc.stop()
  }

  /**
   * @param labelType  0 = regression with labels in [0,1].  Values >= 2 indicate classification.
   * @param fracCategorical  Fraction of columns/features to be categorical.
   * @param fracBinary   Fraction of categorical features to be binary.  Others are high-arity (20).
   * @param treeDepth  Depth of "true" tree used to label points.
   * @return (data, categoricalFeaturesInfo)
   *         data is an RDD of data points.
   *         categoricalFeaturesInfo is a map storing the arity of categorical features.
   *         E.g., an entry (n -> k) indicates that feature n is categorical
   *         with k categories indexed from 0: {0, 1, ..., k-1}.
   */
  def generateDecisionTreeLabeledPoints(
      sc: SparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: Int,
      labelType: Int,
      fracCategorical: Double,
      fracBinary: Double,
      treeDepth: Int,
      seed: Long = System.currentTimeMillis()): RDD[LabeledPoint] = {

    val highArity = 20

    require(fracCategorical >= 0 && fracCategorical <= 1,
      s"fracCategorical must be in [0,1], but it is $fracCategorical")
    require(fracBinary >= 0 && fracBinary <= 1,
      s"fracBinary must be in [0,1], but it is $fracBinary")

    val isRegression = labelType == 0
    if (!isRegression) {
      require(labelType >= 2, s"labelType must be >= 2 for classification. 0 indicates regression.")
    }
    val numCategorical = (numCols * fracCategorical).toInt
    val numContinuous = numCols - numCategorical
    val numBinary = (numCategorical * fracBinary).toInt
    val numHighArity = numCategorical - numBinary
    val categoricalArities = Array.concat(Array.fill(numBinary)(2),
      Array.fill(numHighArity)(highArity))

    val featuresGenerator = new FeaturesGenerator(categoricalArities, numContinuous)
    val featureMatrix = RandomRDDs.randomRDD(sc, featuresGenerator,
      numRows, numPartitions, seed)

    // Create random DecisionTree.
    val featureArity = Array.concat(categoricalArities, Array.fill(numContinuous)(0))
    val trueModel = randomBalancedDecisionTree(treeDepth, labelType, featureArity, seed)
    println(trueModel)

    // Label points using tree.
    val labelVector = featureMatrix.map(trueModel.predict)

    val data = labelVector.zip(featureMatrix).map(pair => new LabeledPoint(pair._1, pair._2))
    val categoricalFeaturesInfo = featuresGenerator.getCategoricalFeaturesInfo
    data
  }

  def randomBalancedDecisionTree(
      depth: Int,
      labelType: Int,
      featureArity: Array[Int],
      seed: Long = System.currentTimeMillis()): DecisionTreeModel = {

    require(depth >= 0, s"randomBalancedDecisionTree given depth < 0.")
    require(depth <= featureArity.size,
      s"randomBalancedDecisionTree requires depth <= featureArity.size," +
      s" but depth = $depth and featureArity.size = ${featureArity.size}")
    val isRegression = labelType == 0
    if (!isRegression) {
      require(labelType >= 2, s"labelType must be >= 2 for classification. 0 indicates regression.")
    }

    val rng = new scala.util.Random()
    rng.setSeed(seed)

    val labelGenerator = if (isRegression) {
      new RealLabelPairGenerator()
    } else {
      new ClassLabelPairGenerator(labelType)
    }

    val topNode = randomBalancedDecisionTreeHelper(0, depth, featureArity, labelGenerator,
      Set.empty, rng)
    if (isRegression) {
      new DecisionTreeModel(topNode, Algo.Regression)
    } else {
      new DecisionTreeModel(topNode, Algo.Classification)
    }
  }

 /**
   * Create an internal node.  Either create the leaf nodes beneath it, or recurse as needed.
   * @param nodeIndex  Index of node.
   * @param subtreeDepth  Depth of subtree to build.  Depth 0 means this is a leaf node.
   * @param featureArity  Indicates feature type.  Value 0 indicates continuous feature.
   *                      Other values >= 2 indicate a categorical feature,
   *                      where the value is the number of categories.
   * @param usedFeatures  Features appearing in the path from the tree root to the node
   *                      being constructed.
   * @param labelGenerator  Generates pairs of distinct labels.
   * @return
   */
  def randomBalancedDecisionTreeHelper(
      nodeIndex: Int,
      subtreeDepth: Int,
      featureArity: Array[Int],
      labelGenerator: RandomDataGenerator[Pair[Double, Double]],
      usedFeatures: Set[Int],
      rng: scala.util.Random): Node = {

    if (subtreeDepth == 0) {
      // This case only happens for a depth 0 tree.
      return new Node(id = nodeIndex, predict = new Predict(0), impurity = 0, isLeaf = true,
        split = None, leftNode = None, rightNode = None, stats = None)
    }

    val numFeatures = featureArity.size
    if (usedFeatures.size >= numFeatures) {
      // Should not happen.
      throw new RuntimeException(s"randomBalancedDecisionTreeSplitNode ran out of " +
        s"features for splits.")
    }

    // Make node internal.
    var feature: Int = rng.nextInt(numFeatures)
    while (usedFeatures.contains(feature)) {
      feature = rng.nextInt(numFeatures)
    }
    val split: Split = if (featureArity(feature) == 0) {
      // continuous feature
      new Split(feature = feature, threshold = rng.nextDouble(),
        featureType = FeatureType.Continuous, categories = List())
    } else {
      // categorical feature
      // Put nCatsSplit categories on left, and the rest on the right.
      // nCatsSplit is in {1,...,arity-1}.
      val nCatsSplit = rng.nextInt(featureArity(feature) - 1) + 1
      val splitCategories = rng.shuffle(Range(0,featureArity(feature)).toList).take(nCatsSplit)
      new Split(feature = feature, threshold = 0,
        featureType = FeatureType.Categorical, categories =
          splitCategories.asInstanceOf[List[Double]])
    }

    val leftChildIndex = nodeIndex * 2 + 1
    val rightChildIndex = nodeIndex * 2 + 2
    if (subtreeDepth == 1) {
      // Add leaf nodes.
      val predictions = labelGenerator.nextValue()
      new Node(id = nodeIndex, predict = new Predict(0), impurity = 0, isLeaf = false, split = Some(split),
        leftNode = Some(new Node(id = leftChildIndex, predict = new Predict(predictions._1), impurity = 0, isLeaf = true,
          split = None, leftNode = None, rightNode = None, stats = None)),
        rightNode = Some(new Node(id = rightChildIndex, predict = new Predict(predictions._2), impurity = 0, isLeaf = true,
          split = None, leftNode = None, rightNode = None, stats = None)), stats = None)
    } else {
      new Node(id = nodeIndex, predict = new Predict(0), impurity = 0, isLeaf = false, split = Some(split),
        leftNode = Some(randomBalancedDecisionTreeHelper(leftChildIndex, subtreeDepth - 1,
          featureArity, labelGenerator, usedFeatures + feature, rng)),
        rightNode = Some(randomBalancedDecisionTreeHelper(rightChildIndex, subtreeDepth - 1,
          featureArity, labelGenerator, usedFeatures + feature, rng)), stats = None)
    }
  }

}

/**
 * Generator for a pair of distinct class labels from the set {0,...,numClasses-1}.
 * @param numClasses  Number of classes.
 */
class ClassLabelPairGenerator(val numClasses: Int)
  extends RandomDataGenerator[Pair[Double, Double]] {

  require(numClasses >= 2,
    s"ClassLabelPairGenerator given label numClasses = $numClasses, but numClasses should be >= 2.")

  private val rng = new java.util.Random()

  override def nextValue(): Pair[Double, Double] = {
    val left = rng.nextInt(numClasses)
    var right = rng.nextInt(numClasses)
    while (right == left) {
      right = rng.nextInt(numClasses)
    }
    new Pair[Double, Double](left, right)
  }

  override def setSeed(seed: Long) {
    rng.setSeed(seed)
  }

  override def copy(): ClassLabelPairGenerator = new ClassLabelPairGenerator(numClasses)
}

/**
 * Generator for a pair of real-valued labels.
 */
class RealLabelPairGenerator() extends RandomDataGenerator[Pair[Double, Double]] {

  private val rng = new java.util.Random()

  override def nextValue(): Pair[Double, Double] =
    new Pair[Double, Double](rng.nextDouble(), rng.nextDouble())

  override def setSeed(seed: Long) {
    rng.setSeed(seed)
  }

  override def copy(): RealLabelPairGenerator = new RealLabelPairGenerator()
}

/**
 * Generator for a feature vector which can include a mix of categorical and continuous features.
 * @param categoricalArities  Specifies the number of categories for each categorical feature.
 * @param numContinuous  Number of continuous features.  Feature values are in range [0,1].
 */
class FeaturesGenerator(val categoricalArities: Array[Int], val numContinuous: Int)
  extends RandomDataGenerator[Vector] {

  categoricalArities.foreach { arity =>
    require(arity >= 2, s"FeaturesGenerator given categorical arity = $arity, " +
      s"but arity should be >= 2.")
  }

  val numFeatures = categoricalArities.size + numContinuous

  private val rng = new java.util.Random()

  /**
   * Generates vector with categorical features first, and continuous features in [0,1] second.
   */
  override def nextValue(): Vector = {
    // Feature ordering matches getCategoricalFeaturesInfo.
    val arr = new Array[Double](numFeatures)
    var j = 0
    while (j < categoricalArities.size) {
      arr(j) = rng.nextInt(categoricalArities(j))
      j += 1
    }
    while (j < numFeatures) {
      arr(j) = rng.nextDouble()
      j += 1
    }
    Vectors.dense(arr)
  }

  override def setSeed(seed: Long) {
    rng.setSeed(seed)
  }

  override def copy(): FeaturesGenerator = new FeaturesGenerator(categoricalArities, numContinuous)

  /**
   * @return categoricalFeaturesInfo Map storing arity of categorical features.
   *                                 E.g., an entry (n -> k) indicates that feature n is categorical
   *                                 with k categories indexed from 0: {0, 1, ..., k-1}.
   */
  def getCategoricalFeaturesInfo: Map[Int, Int] = {
    // Categorical features are indexed from 0 because of the implementation of nextValue().
    categoricalArities.zipWithIndex.map(_.swap).toMap
  }
}


