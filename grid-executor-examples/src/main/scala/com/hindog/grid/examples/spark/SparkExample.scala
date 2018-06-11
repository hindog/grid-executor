package com.hindog.grid.examples.spark

import com.hindog.grid._
import com.hindog.grid.hadoop.HDFSRepository
import com.hindog.grid.launch.{RemoteLaunchArgs, RemoteLaunch}
import com.hindog.grid.spark.SparkLauncher
import org.apache.spark.SparkConf
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

  override def run(args: Array[String]): Unit = {
  
    println("Program Args: " + args.mkString(" "))

    val conf = configure(args, new SparkConf(true))
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.parallelize(0 to 10, 10)
    rdd.map(i => "[executor: " + InetAddress.getLocalHost.getHostName + "]: " + i).collect().foreach(println)
    sc.stop()
    
  }
}

