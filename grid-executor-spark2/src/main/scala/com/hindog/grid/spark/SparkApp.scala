package com.hindog.grid.spark

import com.hindog.grid.Configurable
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.collection._

/**
  * Created by atom-aaron on 6/3/17
  */
trait SparkApp extends Configurable {

  def run(args: Array[String], spark: SparkSession, sc: SparkContext): Unit
  
}
