package com.hindog.grid
package spark

import com.hindog.grid.repo._
import com.hindog.grid.spark.SparkRunner.{ArgumentBuilder, SparkArgument}
import com.typesafe.scalalogging.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION

import scala.collection._

import java.io.File
import java.lang.management.ManagementFactory
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
  
  import SparkRunner._
  
  @transient private val arguments = mutable.ListBuffer[ArgumentBuilder[this.type]]()

  def arg(submitArg: String, confKey: String): Unit = arguments += SparkArgument[this.type](Option(submitArg), Option(confKey), None, _.conf.getOption(confKey))
  def arg(submitArg: String, accessor: SparkRunner => Option[String]): Unit = arguments += SparkArgument[this.type](Option(submitArg), None, None, accessor)
  def flag(submitFlag: String, confKey: String): Unit = arguments += SparkArgument[this.type](None, Option(confKey), Option(submitFlag), _.conf.getOption(confKey).map(_.toLowerCase))
  def flag(submitFlag: String): Unit = arguments += SparkArgument[this.type](None, None, Option(submitFlag), _ => Some("true"))
  def flag(submitFlag: String, accessor: SparkRunner => Boolean): Unit = arguments += SparkArgument[this.type](None, None, Option(submitFlag), runner => if (accessor(runner)) Some("true") else None)
  
  flag("--verbose",             "spark.submit.verbose")
  arg("--class",                v => Option(v.mainClass))
  arg("--master",               "spark.master")
  arg("--deploy-mode",          "spark.submit.deployMode")
  arg("--name",                 "spark.app.name")
  arg("--jars",                 "spark.jars")
  arg("--packages",             "spark.jars.packages")
  arg("--exclude-packages",     "spark.jars.excludes")
  arg("--files",                "spark.files")
  arg("--driver-memory",        "spark.driver.memory")
  arg("--driver-java-options",  "spark.driver.extraJavaOptions")
  arg("--driver-library-path",  "spark.driver.extraLibraryPath")
  arg("--driver-class-path",    "spark.driver.extraClassPath")
  arg("--executor-memory",      "spark.executor.memory")
  arg("--driver-cores",         "spark.driver.cores")
  arg("--queue",                "spark.yarn.queue")
  arg("--num-executors",        "spark.executor.instances")
  arg("--archives",             "spark.yarn.dist.archives")
  arg("--principal",            "spark.yarn.principal")
  arg("--keytab",               "spark.yarn.keytab")
  flag("--supervise",           "spark.driver.supervise")

  @transient lazy val conf: SparkConf = new SparkConf(true)

  /**
    * Utility method to create managed SparkSession that will:
    *
    *   auto-detect Hive libraries and enable hive-support, if requested
    *   auto-stop any running Spark contexts
    *
    */
  def createSparkSession: SparkSession = {

    val execUri = System.getenv("SPARK_EXECUTOR_URI")

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

  def grid: GridConfig
  def repository: Option[Repository] = None

  def mainClass: String = getClass.getName.stripSuffix("$")
  def applicationJar: String = System.getProperty("java.class.path").split(File.pathSeparator).head
  
  def clusterClasspath: Iterable[String] = {
    val repo = repository
    val classpath = clusterClasspathFilter(ClasspathUtils.listCurrentClasspath.flatMap(u => Resource.parse(u.toURI)))
    classpath.map(cp => repo.flatMap(r => Option(r.resolve(cp))).getOrElse(cp.uri)).map(_.toString)
  }
  def clusterClasspathFilter: Iterable[Resource] => Iterable[Resource] = identity

  def shellCommand: Iterable[String] = Seq("/bin/bash", "spark-submit")
  def submitCommand(args: Array[String]): Array[String] = {
    if (clusterClasspath.nonEmpty && !conf.contains("spark.jars") && !conf.contains("spark.yarn.jars")) {
      conf.set("spark.jars", clusterClasspath.mkString(","))
    }

    (shellCommand ++ arguments.flatMap(_.apply(this)) ++ Seq(applicationJar)).toArray ++ args
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

object SparkRunner {

  type ArgumentBuilder[T <: SparkRunner] = T => Iterable[String]
  
  case class SparkArgument[T <: SparkRunner](submitArg: Option[String], confKey: Option[String], flag: Option[String], accessor: T => Option[String]) extends (T => Iterable[String]) with Serializable {
    def apply(instance: T): Iterable[String] = {
      accessor(instance).map(value =>
        // if we have a flag, then we have slightly different behavior--
        // we pass the flag argument with no value
        (if (flag.isDefined) {
          if (accessor(instance).map(_.toLowerCase).contains("true")) {
            flag.toIterable
          } else {
            Iterable.empty
          }
        } else {
          submitArg.fold(Iterable.empty[String])(arg => Iterable(arg, value))
        }) ++ confKey.fold(Iterable.empty[String])(conf => Iterable("--conf", s"$conf=$value"))
      ).getOrElse(Iterable.empty)
    }
  }

}