package com.hindog.grid.examples.spark

import better.files.File
import com.hindog.grid._
import com.hindog.grid.repo._
import com.hindog.grid.spark.SparkLauncher
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.net.InetAddress
import java.util.Properties

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
object SparkStandaloneExample extends SparkLauncher with Logging {

  override protected def shellCommand: Iterable[String] = Iterable("/opt/spark/spark-2.3.1-bin-hadoop-2.8/bin/spark-submit")

//  @transient override protected lazy val repository: Option[Repository] = {
//    val props = new Properties()
////    props.put("class-name", "com.hindog.grid.repo.http.DelegatingHttpRepository")
////    props.put("hostname", "s3-proxy")
////    props.put("port", "80")
////    props.put("path", "var/jar-cache")
////    props.put("delegate.class-name", "com.hindog.grid.repo.S3Repository")
////    props.put("delegate.bucket", "atom-data-pipeline")
////    props.put("delegate.prefix", "var/jar-cache")
//
//    props.put("class-name", "com.hindog.grid.repo.S3Repository")
//    props.put("bucket", "atom-data-pipeline")
//    props.put("prefix", "var/jar-cache")
//
//    Some(Repository(props))
//  }

  override def applicationJar: String = {
    val appJar = File(super.applicationJar)
    repository match {
      case Some(repo) => {
        repo.put(Resource.file(appJar.name, appJar)).uri.toString
      }
      case None => appJar.uri.toString
    }
  }

  override protected def configure(args: Array[String], conf: SparkConf): SparkConf = {
    conf.set("spark.master", "spark://internal-a2517d648831111e88f1f0ab0e75ad18-1118161659.us-west-2.elb.amazonaws.com:7077")
    conf.set("spark.submit.deployMode", "client")
    conf.set("spark.logConf", "true")
    conf.set("spark.submit.verbose", "true")
    conf.set("spark.hadoop.fs.s3a.awsAccessKeyId", "AKIAJM6JU4VV25PWXRUQ")
    conf.set("spark.hadoop.fs.s3a.awsSecretAccessKey", "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")
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem ")
    conf.set("spark.hadoop.fs.s3a.access.key", "AKIAJM6JU4VV25PWXRUQ")
    conf.set("spark.hadoop.fs.s3a.secret.key", "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")
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", "s3a://atom-data-pipeline/var/event-log/")
    conf.set("spark.executor.instances", "1")
    conf.set("spark.executor.memory", "1g")
    conf.set("spark.cores.max", "2")
    conf.set("spark.executor.cores", "1")
    conf.set("spark.dynamicAllocation.enabled", "false")
    conf.set("spark.dynamicAllocation.minExecutors", "1")
    conf.set("spark.dynamicAllocation.maxExecutors", "1")
    conf.set("spark.dynamicAllocation.initialExecutors", "1")
    conf.set("spark.shuffle.service.enabled", "true")
    conf.set("spark.sql.catalogImplementation", "hive")
    conf.set("spark.sql.hive.metastore.jars", "builtin")
    conf.set("spark.dynamicAllocation.maxExecutors", "2")
    conf.set("spark.hadoop.hive.metastore.uris", "thrift://internal-aef0e150c854011e89d4d0610895c55e-1927968424.us-west-2.elb.amazonaws.com:9083")

    conf
  }

  override def run(args: Array[String]): Unit = {
    
    println("Program Args: " + args.mkString(" "))
    
    val conf = configure(args, new SparkConf(true))
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    spark.catalog.listDatabases().show(100)
    
    val rdd = sc.parallelize(0 to 10, 10)
    rdd.map(i => {
      //if (i % 3 == 0) throw new RuntimeException("Triggered Exception")
      println("Executor log: " + "[executor: " + InetAddress.getLocalHost.getHostName + "]: " + i)
      "[executor: " + InetAddress.getLocalHost.getHostName + "]: " + i
    }).collect().foreach(println)
    sc.stop()
    
  }
}

