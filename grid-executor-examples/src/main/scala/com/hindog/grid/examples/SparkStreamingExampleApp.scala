package com.hindog.grid.examples

import java.io.File

import com.hindog.grid.hadoop.HadoopEnvironment
import com.hindog.grid.repo.Repository
import com.hindog.grid.spark.{AbstractSparkRunner, SparkStreamingApp}
import com.hindog.grid.{GridConfig, RemoteNodeConfig}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.streaming.{Minutes, StreamingContext}

import scala.collection._

/**
  * Created by atom-aaron on 6/3/17
  */
class SparkStreamingExampleApp extends SparkStreamingApp with LazyLogging {

  conf(_.set("spark.streaming.fileStream.minRememberDuration", "24h"))
  
  override def checkpointDir = Some("/tmp/checkpoint/")

  override protected def run(args: Array[String], spark: SparkSession, ssc: StreamingContext): Unit = {

    val dir = "/tmp/streaming/"

    val fs = FileSystem.get(HadoopEnvironment.loadConfiguration())
    fs.mkdirs(new Path("/tmp/checkpoint/"))
    
//    fs.delete(new Path(dir), true)
//    fs.mkdirs(new Path(dir))
//
    val input = ssc.fileStream[LongWritable, Text, TextInputFormat](dir, (f: Path) => !f.getName.startsWith("_"), false)
    
    ssc.remember(Minutes(60 * 24))

    ssc.addStreamingListener(new StreamingListener {
      override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
        println("completed batch: " + batchCompleted.batchInfo.streamIdToInputInfo)
      }
    })

    val count = input.mapPartitions(itr => Iterator(itr.size))
    count.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        println("processed files with line counts: " + rdd.collect().mkString(","))
      }
    })

    ssc.start()

    ssc.awaitTerminationOrTimeout(60000)
  }
  
}

object SparkStreamingExampleAppRunner extends AbstractSparkRunner[SparkStreamingExampleApp] {
  override def master: String = "yarn"

  override def repository = Some(Repository.hdfs)

  override def grid: GridConfig = GridConfig.apply("spark-shell-example",
    RemoteNodeConfig("10.0.2.127")
      .withSSHAccount("hadoop")
      .withSSHKey(new File("~/.ssh/devKeyPair.pem"))
      .withInheritedEnv("AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY", "AWS_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"))
      .withDebugServer(suspend = true)
}