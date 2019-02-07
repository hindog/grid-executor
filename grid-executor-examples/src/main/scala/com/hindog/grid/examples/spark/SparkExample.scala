package com.hindog.grid.examples.spark

import com.hindog.grid._
import com.hindog.grid.spark._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import java.net.InetAddress

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
object SparkExample extends SparkLauncher with Logging {

  override protected def configureLaunch(config: SparkLauncher.Config): SparkLauncher.Config = {
    config.withGridConfig(_.withNodes(LocalNodeConfig("local")))
  }

  override def launch(args: Array[String]): Unit = {
  
    println("Program Args: " + args.mkString(" "))

    val conf = new SparkConf(true)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.parallelize(0 to 10, 10)
    rdd.map(i => "[executor: " + InetAddress.getLocalHost.getHostName + "]: " + i).collect().foreach(println)
    sc.stop()
  }

  override def run(args: Array[String])(implicit spark: SparkSession, sc: SparkContext): Unit = ???
}

