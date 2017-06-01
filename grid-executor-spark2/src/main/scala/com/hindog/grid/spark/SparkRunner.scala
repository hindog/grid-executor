package com.hindog.grid
package spark

import java.io.File
import java.lang.management.ManagementFactory
import java.net.URI

import com.hindog.grid._
import com.hindog.grid.repo.{Repository, Resource, SyncRepositoryHook}
import com.typesafe.scalalogging.Logger
import org.apache.spark.repl.Main.{conf, outputDir}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 *
 * TODO: forking for embedded use
 */
trait SparkRunner {

  @transient private lazy val _config = mutable.HashMap[String, String]()

  protected var sparkSession: SparkSession = _
  protected var sparkContext: SparkContext = _

  // Register shutdown hook to stop context automatically if this process is stopped
  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = if (sparkContext != null && !sparkContext.isStopped && sparkContext.getConf.getBoolean("spark.yarn.submit.waitAppCompletion", false)) {
      sparkContext.stop()
    }
  })

  def config(key: String, value: String) = _config += key -> value
  def config(key: String, value: Double) = _config += key -> value.toString
  def config(key: String, value: Long) = _config += key -> value.toString
  def config(key: String, value: Boolean) = _config += key -> value.toString

  def configure: SparkConf => SparkConf = conf => _config.foldLeft(conf)((acc, cur) => conf.set(cur._1, cur._2))

  /**
    * Utility method to create managed SparkSession that will:
    *
    *   auto-detect Hive libraries and enable hive-support, if requested
    *   auto-stop any running Spark contexts
    *
    */
  def createSparkSession: SparkSession = {
    val conf = configure(new SparkConf(true))

    val execUri = System.getenv("SPARK_EXECUTOR_URI")
    conf.setIfMissing("spark.app.name", getClass.getName.stripSuffix("$"))

    
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
        sparkSession = builder.enableHiveSupport().getOrCreate()
      } else {
        // Need to change it back to 'in-memory' if no hive classes are found
        // in the case that the property is set to hive in spark-defaults.conf
        builder.config(CATALOG_IMPLEMENTATION.key, "in-memory")
        sparkSession = builder.getOrCreate()
      }
    } else {
      // In the case that the property is set but not to 'hive', the internal
      // default is 'in-memory'. So the sparkSession will use in-memory catalog.
      sparkSession = builder.getOrCreate()
    }
    sparkContext = sparkSession.sparkContext

    sparkSession
  }

  protected def hiveClassesArePresent: Boolean = {
    val HIVE_SESSION_STATE_CLASS_NAME = "org.apache.spark.sql.hive.HiveSessionState"
    try {
      Class.forName(HIVE_SESSION_STATE_CLASS_NAME)
      Class.forName("org.apache.hadoop.hive.conf.HiveConf")
      true
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError => false
    }
  }

  def deployMode: String = "cluster"
  def verbose: Boolean = false
  def master: String = "yarn"
  def proxyUser: String = ""
  def queue: String = ""
  def files: Iterable[URI] = Iterable.empty
  def driverVMOptions: String = ""
  def driverMemory: String = ""
  def driverLibraryPath: String = ""
  def driverClasspath: String = ""
  def driverCores: Option[Int] = None

  def jarFilter: Iterable[Resource] => Iterable[Resource] = identity
  def assemblyArchive: Option[URI] = None

  def grid: GridConfig
  def repository: Option[Repository] = None

  def main(args: Array[String]): Unit = {
    /*
      Detect if we are running via spark-submit, if so, run as normal, otherwise, invoke remote launch...
     */
    if ("true" == System.getProperty("SPARK_SUBMIT") || System.getenv("SPARK_YARN_MODE") != null) {
      run(args)
    } else {
      val repo = repository
      val gridConfig = grid.ifDefinedThen(repo)((g, repo) => g.addStartupHook(new SyncRepositoryHook(repo)))
      val mainClass = getClass.getName.stripSuffix("$")

      GridExecutor.withInstance(gridConfig) { executor =>
        val task = executor.submit(new Runnable with Serializable {
          override def run(): Unit = {
            Logger(mainClass).info(s"Running $mainClass remotely under process: ${ManagementFactory.getRuntimeMXBean.getName}")

            val classpath = jarFilter(ClasspathUtils.listCurrentClasspath.flatMap(u => Resource.parse(u.toURI)))
            val jars = classpath.map(cp => repo.flatMap(r => Option(r.resolve(cp))).getOrElse(cp.uri)).map(_.toString).mkString(",")

            val args = Array(
              "/bin/bash", "spark-submit",
              "--master", master,
              "--deploy-mode", deployMode,
              "--class", mainClass,
              "--jars", jars
            ).ifThen(verbose)(_ :+ "--verbose")
             .ifThen(assemblyArchive.isDefined)(_ ++ Array("--conf", "spark.yarn.archive=" + assemblyArchive.get.toString))
             .ifThen(files.nonEmpty)(_ ++ Array("--files", files.map(_.toString).mkString(",")))
             .ifThen(driverClasspath.nonEmpty)(_ ++ Array("--driver-class-path", driverClasspath))
             .ifThen(driverMemory.nonEmpty)(_ ++ Array("--driver-memory", driverMemory))
             .ifThen(driverLibraryPath.nonEmpty)(_ ++ Array("--driver-library-path", driverLibraryPath))
             .ifThen(driverVMOptions.nonEmpty)(_ ++ Array("--driver-java-options", driverVMOptions))
             .ifThen(driverCores.nonEmpty)(_ ++ Array("--driver-cores", driverVMOptions))
             .ifThen(queue.nonEmpty)(_ ++ Array("--queue", queue))
             .ifThen(_config.nonEmpty)(_ ++ _config.flatMap(kv => Array("--conf", s"${kv._1}=${kv._2}"))) :+ System.getProperty("java.class.path").split(File.pathSeparator).head

            val process = new ProcessBuilder(args: _*).inheritIO().start()
            process.waitFor()
          }
        })
        task.get()
        // pause a bit to wait for StdOut/StdErr streams
        Thread.sleep(1000)
      }
    }
  }

  def run(args: Array[String])

}