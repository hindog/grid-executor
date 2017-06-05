package com.hindog.grid.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

import scala.collection._
import scala.concurrent.duration._

/**
  * Created by atom-aaron on 6/3/17
  */
trait SparkStreamingApp extends SparkApp {

  def checkpointDir: Option[String] = None
  def duration: Duration = 10 seconds

  override def run(args: Array[String], spark: SparkSession, sc: SparkContext): Unit = {
    val streamingContext = checkpointDir match {
      case Some(chk) => StreamingContext.getOrCreate(chk, () => create(args, sc))
      case None => create(args, sc)
    }

    run(args, spark, streamingContext)
  }

  protected def create(args: Array[String], sc: SparkContext): StreamingContext = new StreamingContext(sc, org.apache.spark.streaming.Duration.apply(duration.toMillis))

  protected def run(args: Array[String], spark: SparkSession, ssc: StreamingContext): Unit
}
