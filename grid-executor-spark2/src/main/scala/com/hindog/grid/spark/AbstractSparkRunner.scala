package com.hindog.grid.spark

import com.hindog.grid.Config
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION

import scala.reflect.runtime.universe._
import scala.collection._
import scala.reflect.ClassTag

/**
  * Created by atom-aaron on 6/3/17
  */
abstract class AbstractSparkRunner[T <: SparkApp](implicit ct: ClassTag[T]) extends SparkRunner {

  override def run(args: Array[String]): Unit = {

    println("Instantiating " + ct.runtimeClass.toString)
    val app = ct.runtimeClass.newInstance().asInstanceOf[SparkApp]

    println("Instantiating " + ct.runtimeClass.toString + " DONE!")
    
    val conf = {
      val c = (config ++ app.config).foldLeft(Config("spark"))((acc, cur) => cur(acc))
      val sparkConf = new SparkConf(true)
      sparkConf.setAll(c.getAll.toSeq)
      sparkConf
    }

    val execUri = System.getenv("SPARK_EXECUTOR_URI")
    conf.setIfMissing("spark.app.name", getClass.getName.stripSuffix("$"))

    // SparkContext will detect this configuration and register it with the RpcEnv's
    // file server, setting spark.repl.class.uri to the actual URI for executors to
    // use. This is sort of ugly but since executors are started as part of SparkContext
    // initialization in certain cases, there's an initialization order issue that prevents
    // this from being set after SparkContext is instantiated.

    if (execUri != null) {
      conf.set("spark.executor.uri", execUri)
    }
    if (System.getenv("SPARK_HOME") != null) {
      conf.setSparkHome(System.getenv("SPARK_HOME"))
    }

    val builder = SparkSession.builder.config(conf)

    val sparkSession = if (conf.get(CATALOG_IMPLEMENTATION.key, "hive").toLowerCase == "hive") {
      if (hiveClassesArePresent) {
        // In the case that the property is not set at all, builder's config
        // does not have this value set to 'hive' yet. The original default
        // behavior is that when there are hive classes, we use hive catalog.
        builder.enableHiveSupport().getOrCreate()
      } else {
        // Need to change it back to 'in-memory' if no hive classes are found
        // in the case that the property is set to hive in spark-defaults.conf
        builder.config(CATALOG_IMPLEMENTATION.key, "in-memory")
        builder.getOrCreate()
      }
    } else {
      // In the case that the property is set but not to 'hive', the internal
      // default is 'in-memory'. So the sparkSession will use in-memory catalog.
      builder.getOrCreate()
    }

    app.run(args, sparkSession, sparkSession.sparkContext)
  }
}
