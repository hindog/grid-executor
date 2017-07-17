package com.hindog.grid.examples

import com.hindog.grid.hadoop.HDFSRepository
import com.hindog.grid.spark.SparkRunner
import com.hindog.grid.{GridConfig, RemoteNodeConfig}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import java.io.File
import java.net.InetAddress

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
object SparkExample extends SparkRunner {

  override def sparkSubmit = "spark2-submit"
  override def master: String = "yarn"
  override def verbose = true

  override def grid: GridConfig = GridConfig.apply("spark-example",
                                    RemoteNodeConfig("10.0.1.148")
                                    .withInheritedEnv("AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY", "AWS_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"))


  override def repository = Some(HDFSRepository())

  override def run(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Test Spark App")
    val sc = SparkContext.getOrCreate(conf)

    val rdd = sc.parallelize(0 to 10, 10)
    rdd.map(i => InetAddress.getLocalHost.getHostName + ": " + i).collect().foreach(println)
    sc.stop()
    
  }
}

