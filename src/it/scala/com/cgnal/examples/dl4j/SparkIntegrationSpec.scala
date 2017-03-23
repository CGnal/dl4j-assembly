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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpec}

class SparkIntegrationSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
  var sparkContext: SparkContext = _

  def getJar(klass: Class[_]): String = {
    val codeSource = klass.getProtectionDomain.getCodeSource
    codeSource.getLocation.getPath
  }

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  override def beforeAll(): Unit = {

    val initialExecutors = 4

    val minExecutors = 4

    val hadoop_conf_dir = Option(System.getenv("HADOOP_CONF_DIR"))

    hadoop_conf_dir.fold(Predef.assert(false, "please set the HADOOP_CONF_DIR env variable"))(addPath(_))

    val uberJarLocation = s"${System.getProperty("user.dir")}/assembly/target/scala-2.10/spark-cdh-template-assembly-1.0.jar"

    val conf = new SparkConf().
      setMaster("yarn-client").
      setAppName("spark-cdh5-template-yarn").
      setJars(List(uberJarLocation)).
      set("spark.yarn.jar", "local:/opt/cloudera/parcels/CDH/lib/spark/assembly/lib/spark-assembly.jar").
      set("spark.executor.extraClassPath", "/opt/cloudera/parcels/CDH/jars/*").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      set("spark.io.compression.codec", "lzf").
      set("spark.speculation", "true").
      set("spark.shuffle.manager", "sort").
      set("spark.shuffle.service.enabled", "true").
      set("spark.dynamicAllocation.enabled", "true").
      set("spark.dynamicAllocation.initialExecutors", Integer.toString(initialExecutors)).
      set("spark.dynamicAllocation.minExecutors", Integer.toString(minExecutors)).
      set("spark.executor.cores", Integer.toString(1)).
      set("spark.executor.memory", "256m")

    sparkContext = new SparkContext(conf)
  }

  "Spark" must {
    "load an avro file as a schema rdd correctly" in {

    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  override def afterAll(): Unit = {
    sparkContext.stop()
  }

}
