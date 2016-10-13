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

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, File }
import java.util.UUID

import org.apache.hadoop.io.{ BytesWritable, Text }
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.deeplearning4j.datasets.iterator.impl.MnistDataSetIterator
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.layers.{ DenseLayer, OutputLayer }
import org.deeplearning4j.nn.conf.{ NeuralNetConfiguration, Updater }
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.spark.api.{ Repartition, RepartitionStrategy }
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer
import org.deeplearning4j.spark.impl.paramavg.ParameterAveragingTrainingMaster
import org.deeplearning4j.spark.stats.StatsUtils
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.lossfunctions.LossFunctions

// spark-submit --master yarn --deploy-mode client --class com.cgnal.examples.dl4j.Main dl4j-example-assembly-1.0.jar

object Main extends App {

  val yarn = true

  val initialExecutors = 4

  val minExecutors = 4

  private val conf = new SparkConf().setAppName("dl4j-example-yarn")

  private val master = conf.getOption("spark.master")

  private val uberJarLocation = {
    val location = getJar(Main.getClass)
    if (new File(location).isDirectory) s"${System.getProperty("user.dir")}/assembly/target/scala-2.10/dl4j-example-assembly-1.0.jar" else location
  }

  if (master.isEmpty) {
    //it means that we are NOT using spark-submit

    addPath(args(0))

    if (yarn) {
      val _ = conf.
        setMaster("yarn-client").
        setAppName("dl4j-example-local").
        setJars(List(uberJarLocation)).
        set("spark.yarn.jar", "local:/opt/cloudera/parcels/CDH/lib/spark/assembly/lib/spark-assembly.jar").
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
        setMaster("local[*]")
    }
  }

  val _ = conf.
    set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
    set("spark.kryo.registrator", "org.nd4j.Nd4jRegistrator")

  val sparkContext = new SparkContext(conf)

  sparkContext.setLogLevel("ERROR")

  val batchSizePerWorker = 32
  val seed = 12345

  import collection.convert.decorateAsScala._

  private val iter = new MnistDataSetIterator(batchSizePerWorker, true, seed)
  private val data = iter.asScala.toList

  private val rdd: RDD[DataSet] = sparkContext.parallelize[DataSet](data)

  private val forSequenceFile = rdd.map(dataset => {
    val baos = new ByteArrayOutputStream()
    dataset.save(baos)
    val bytes = baos.toByteArray
    (new Text(UUID.randomUUID().toString), new BytesWritable(bytes))
  })
  forSequenceFile.saveAsSequenceFile("./MnistMLPPreprocessed", None)

  private val sequenceFile = sparkContext.sequenceFile("./MnistMLPPreprocessed", classOf[Text], classOf[BytesWritable])
  private val trainData = sequenceFile.map(pair => {
    val ds = new DataSet()
    val bais = new ByteArrayInputStream(pair._2.getBytes)
    ds.load(bais)
    ds
  })

  private val nnconf = new NeuralNetConfiguration.Builder()
    .seed(12345)
    .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
    .iterations(1)
    .activation("relu")
    .weightInit(WeightInit.XAVIER)
    .learningRate(0.0069)
    .updater(Updater.NESTEROVS).momentum(0.9)
    .regularization(true).l2(1e-4)
    .list()
    .layer(0, new DenseLayer.Builder().nIn(28 * 28).nOut(500).build())
    .layer(1, new DenseLayer.Builder().nIn(500).nOut(100).build())
    .layer(2, new OutputLayer.Builder(LossFunctions.LossFunction.NEGATIVELOGLIKELIHOOD)
      .activation("softmax").nIn(100).nOut(10).build())
    .pretrain(false).backprop(true)
    .build()

  private val tm = new ParameterAveragingTrainingMaster.Builder(batchSizePerWorker)
    .averagingFrequency(10)
    .saveUpdater(true)
    .workerPrefetchNumBatches(2)
    .batchSizePerWorker(batchSizePerWorker)
    .repartionData(Repartition.Always)
    .repartitionStrategy(RepartitionStrategy.SparkDefault)
    .build()

  private val sparkNet = new SparkDl4jMultiLayer(sparkContext, nnconf, tm)
  sparkNet.setCollectTrainingStats(true)

  for (i <- 0 to 10) {
    val _ = sparkNet.fit(trainData)
    println(s"Completed Epoch $i")
  }

  private val stats = sparkNet.getSparkTrainingStats
  StatsUtils.exportStatsAsHtml(stats, "SparkStats.html", sparkContext)

  sparkContext.stop()

}
