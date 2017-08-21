package com.hindog.grid
package spark

import com.hindog.grid._
import com.hindog.grid.repo._
import com.typesafe.scalalogging.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION

import java.io.File
import java.lang.management.ManagementFactory
import java.net.URI
import java.util.concurrent.Callable

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 *
 * TODO: forking for embedded use
 */
abstract class SparkRunner { parent =>

  @transient lazy val conf: SparkConf = configure(new SparkConf(true))

  def configure: SparkConf => SparkConf = identity

  /**
    * Utility method to create managed SparkSession that will:
    *
    *   auto-detect Hive libraries and enable hive-support, if requested
    *   auto-stop any running Spark contexts
    *
    */
  def createSparkSession: SparkSession = {

    val execUri = System.getenv("SPARK_EXECUTOR_URI")
    conf.setIfMissing("spark.app.name", appName.getOrElse(getClass.getName.stripSuffix("$")))

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
    if (conf.get(CATALOG_IMPLEMENTATION.key, "hive").toLowerCase == "hive") {
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

  }

  protected def hiveClassesArePresent: Boolean = {
    try {
      Class.forName("org.apache.hadoop.hive.conf.HiveConf")
      true
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError => false
    }
  }

  def sparkSubmit: String = "spark-submit"
  def master: String
  def grid: GridConfig

  def jars: Array[String] = {
    val repo = repository
    val classpath = jarFilter(ClasspathUtils.listCurrentClasspath.flatMap(u => Resource.parse(u.toURI)))
    classpath.map(cp => repo.flatMap(r => Option(r.resolve(cp))).getOrElse(cp.uri)).map(_.toString).toArray
  }

  def mainClass: String = getClass.getName.stripSuffix("$")
  def appName: Option[String] = None
  def deployMode: String = "client"
  def verbose: Boolean = false
  def proxyUser: Option[String] = None
  def queue: Option[String] = None
  def files: Iterable[URI] = Iterable.empty
  def driverJavaOptions: Option[String] = None
  def driverMemory: Option[String] = None
  def driverLibraryPath: Option[String] = None
  def driverClasspath: Option[String] = None
  def driverCores: Option[Int] = None

  def jarFilter: Iterable[Resource] => Iterable[Resource] = identity
  def assemblyArchive: Option[URI] = None
  def repository: Option[Repository] = None

  def submitCommand(args: Array[String]): Array[String] = {

    implicit class Args(arr: Array[String]) {
      def arg[T](f: SparkRunner => Option[T], flag: String): Array[String] = {
        f(SparkRunner.this).map(o => arr ++ Array(flag, o.toString)).getOrElse(arr)
      }

      def conf[T](f: SparkRunner => Option[T], conf: String): Array[String] = {
        f(SparkRunner.this).map(o => arr ++ Array("--conf", conf + "=" + o.toString)).getOrElse(arr)
      }
    }

    Array(
      "/bin/bash", sparkSubmit,
      "--master", master,
      "--deploy-mode", deployMode,
      "--class", mainClass,
      "--jars", jars.mkString(",")
    ).ifThen(verbose)(_ :+ "--verbose")
      .arg(f => Option(f.files).filter(_.nonEmpty).map(_.map(_.toString).mkString(",")), "--files")
      .arg(_.driverClasspath, "--driver-class-path")
      .arg(_.driverMemory, "--driver-memory")
      .arg(_.driverLibraryPath, "--driver-library-path")
      .arg(_.driverJavaOptions, "--driver-java-options")
      .arg(_.driverCores, "--driver-cores")
      .arg(_.queue, "--queue")
      .conf(_.appName, "spark.app.name")
      .conf(_.assemblyArchive, "spark.yarn.archive")
      .ifThen(conf.getAll.nonEmpty)(_ ++ conf.getAll.flatMap(kv => Array("--conf", s"${kv._1}=${kv._2}"))) ++
      Array(System.getProperty("java.class.path").split(File.pathSeparator).head) ++
      args
  }

  def main(args: Array[String]): Unit = {
    /*
      Detect if we are running via spark-submit, if so, run as normal, otherwise, invoke remote launch...
     */
    if ("true" == System.getProperty("SPARK_SUBMIT") || System.getenv("SPARK_YARN_MODE") != null) {
      run(args)
    } else {
      val repo = repository
      val gridConfig = grid.ifDefinedThen(repo)((g, repo) => g.addStartupHook(new SyncRepositoryHook(repo)))

      implicit class Args(arr: Array[String]) {
        def arg[T](f: SparkRunner => Option[T], flag: String): Array[String] = {
          f(SparkRunner.this).map(o => arr ++ Array(flag, o.toString)).getOrElse(arr)
        }

        def conf[T](f: SparkRunner => Option[T], conf: String): Array[String] = {
          f(SparkRunner.this).map(o => arr ++ Array("--conf", conf + "=" + o.toString)).getOrElse(arr)
        }
      }

      val retCode = GridExecutor.withInstance(gridConfig) { executor =>
        val task = executor.submit(new Callable[Int] with Serializable {
          override def call(): Int = {
            Logger(mainClass).info(s"Running $mainClass remotely under process: ${ManagementFactory.getRuntimeMXBean.getName}")
            val process = new ProcessBuilder(submitCommand(args): _*).inheritIO().start()
            process.waitFor()
          }
        })

        try {
          task.get()
        } finally {
          // pause a bit to wait for StdOut/StdErr streams
          Thread.sleep(1000)
        }
      }
      
      System.exit(retCode)
    }
  }

  def run(args: Array[String])

}
