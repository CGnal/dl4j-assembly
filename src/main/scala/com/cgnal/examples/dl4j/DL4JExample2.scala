/*
 * Copyright 2016 CGnal S.p.A.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cgnal.examples.dl4j

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.datavec.api.records.reader.impl.csv.CSVRecordReader
import org.datavec.api.split.StringSplit
import org.deeplearning4j.datasets.datavec.RecordReaderDataSetIterator
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.layers.{ DenseLayer, OutputLayer }
import org.deeplearning4j.nn.conf.{ NeuralNetConfiguration, Updater }
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.spark.api.{ Repartition, RepartitionStrategy }
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer
import org.deeplearning4j.spark.impl.paramavg.ParameterAveragingTrainingMaster
import org.nd4j.linalg.activations.Activation
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.lossfunctions.LossFunctions._

// spark-submit --master yarn --deploy-mode client --class com.cgnal.examples.dl4j.DL4JExample2 dl4j-assembly-0.8.0.jar

object DL4JExample2 extends App {

  val yarn = true

  val initialExecutors = 4

  val minExecutors = 4

  private val conf = new SparkConf().setAppName("dl4j-example-yarn")

  private val master = conf.getOption("spark.master")

  private val uberJarLocation = {
    val location = getJar(DL4JExample1.getClass)
    if (new File(location).isDirectory) s"${System.getProperty("user.dir")}/assembly/target/scala-2.11/dl4j-assembly-0.8.0.jar" else location
  }

  if (master.isEmpty) {
    //it means that we are NOT using spark-submit

    addPath(args(0))

    if (yarn) {
      val _ = conf.
        setMaster("yarn-client").
        setAppName("dl4j-example-local").
        setJars(List(uberJarLocation)).
        set("spark.yarn.jars", "local:/opt/cloudera/parcels/SPARK2/lib/spark2/jars/*").
        set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
        set("spark.io.compression.codec", "lzf").
        set("spark.speculation", "true").
        set("spark.shuffle.manager", "sort").
        set("spark.shuffle.service.enabled", "true").
        set("spark.dynamicAllocation.enabled", "true").
        set("spark.dynamicAllocation.initialExecutors", Integer.toString(initialExecutors)).
        set("spark.dynamicAllocation.minExecutors", Integer.toString(minExecutors)).
        set("spark.executor.cores", Integer.toString(1)).
        set("spark.executor.memory", "1200m")
    } else {
      val _ = conf.
        setAppName("dl4j-example-local").
        setMaster("local[16]")
    }
  }

  val _ = conf.
    set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
    set("spark.kryo.registrator", "org.nd4j.Nd4jRegistrator")

  val sparkContext = new SparkContext(conf)

  sparkContext.setLogLevel("ERROR")

  val rawData = sparkContext.textFile("data/mls/ch05/train.tsv")

  val rawDataNoHeader = rawData.filter(line => !line.contains("hasDomainLink"))

  val records = rawDataNoHeader.map(line => line.split("\t"))

  val categories = records.map(r => r(3)).distinct.collect.zipWithIndex.toMap
  val numCategories = categories.size

  val lines = records.
    map { r =>
      val trimmed = r.map(_.replaceAll("\"", "").trim)
      val label = trimmed(r.size - 1)
      val categoryIdx = categories(r(3))
      val categoryFeatures = Array.fill(numCategories)("0.0")
      categoryFeatures(categoryIdx) = "1.0"
      val otherFeatures: Array[String] = trimmed.slice(4, r.size - 1).map(d => if (d == "?") "0.0" else d)
      val features = categoryFeatures ++ otherFeatures
      (label :: features.toList).mkString(",")
    }

  val trainTestSplit = lines.randomSplit(Array(0.6, 0.4), 123)
  val train = trainTestSplit(0)
  val test = trainTestSplit(1)

  val trainDatasets: RDD[DataSet] = train.mapPartitions[DataSet](iterator => {
    val datasets: Iterator[DataSet] = iterator.map(line => {
      val rr = new CSVRecordReader(0, ",")
      rr.initialize(new StringSplit(line))
      val iterator = new RecordReaderDataSetIterator(rr, 1, 0, 2)
      iterator.next()
    })
    import collection.convert.decorateAsJava._
    List(DataSet.merge(datasets.toList.asJava)).iterator
  }).map[DataSet](ds => {
    ds.normalize()
    ds
  })

  val testDatasets: RDD[DataSet] = test.mapPartitions[DataSet](iterator => {
    val datasets: Iterator[DataSet] = iterator.map(line => {
      val rr = new CSVRecordReader(0, ",")
      rr.initialize(new StringSplit(line))
      val iterator = new RecordReaderDataSetIterator(rr, 1, 0, 2)
      iterator.next()
    })
    import collection.convert.decorateAsJava._
    List(DataSet.merge(datasets.toList.asJava)).iterator
  }).map[DataSet](ds => {
    ds.normalize()
    ds
  })

  val iterations = 10
  val seed = 123
  val learningRate = 0.005
  val nEpochs = 20
  val numInputs = 36
  val numOutputs = 2
  val numHiddenNodes = 20
  val batchSizePerWorker = 8

  val nnconf = new NeuralNetConfiguration.Builder().
    seed(seed).
    iterations(iterations).
    optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT).
    learningRate(learningRate).
    updater(Updater.NESTEROVS).momentum(0.9).
    list().
    layer(0, new DenseLayer.Builder().
      nIn(numInputs).
      nOut(numHiddenNodes).
      weightInit(WeightInit.XAVIER).
      activation(Activation.RELU).
      build()).
    layer(1, new OutputLayer.Builder(LossFunction.NEGATIVELOGLIKELIHOOD).
      weightInit(WeightInit.XAVIER).
      activation(Activation.SOFTMAX).
      nIn(numHiddenNodes).nOut(numOutputs).
      build()).
    pretrain(false).backprop(true).build()

  val tm = new ParameterAveragingTrainingMaster.Builder(batchSizePerWorker).
    averagingFrequency(10).
    saveUpdater(true).
    workerPrefetchNumBatches(2).
    batchSizePerWorker(batchSizePerWorker).
    repartionData(Repartition.Always).
    repartitionStrategy(RepartitionStrategy.SparkDefault).
    build()

  val sparkNet = new SparkDl4jMultiLayer(sparkContext, nnconf, tm)

  for (i <- 0 until nEpochs) {
    sparkNet.fit(trainDatasets)
  }

  val evaluation = sparkNet.evaluate(testDatasets)
  println(evaluation.stats())

  sparkContext.stop()

}
