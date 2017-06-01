package com.hindog.grid.examples

import java.io.File

import com.hindog.grid.hadoop.HDFSRepository
import com.hindog.grid.spark.SparkRunner
import com.hindog.grid.{GridConfig, RemoteNodeConfig}
import org.apache.spark.{SparkConf, SparkContext}

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
object SparkExample extends SparkRunner {

  override def master: String = "yarn"

  override def grid: GridConfig = GridConfig.apply("spark-emr",
                                    RemoteNodeConfig("10.0.2.25")
                                    .withSSHAccount("hadoop")
                                    .withSSHKey(new File("~/.ssh/devKeyPair.pem"))
                                    .withInheritedEnv("AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY", "AWS_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"))


  override def repository = Some(HDFSRepository())

  override def run(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Test Spark App")
    val sc = SparkContext.getOrCreate(conf)

    val rdd = sc.parallelize(0 to 10, 10)
    rdd.foreach(i => println(i))
    sc.stop()
  }
}

