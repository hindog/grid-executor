package com.hindog.grid.spark.shell

import org.apache.spark.sql.SparkSession

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
class SparkShutdownHook(spark: SparkSession) extends Thread {
  override def run(): Unit = spark.stop()
}
