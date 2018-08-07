package com.hindog.grid
package spark

import com.hindog.grid.launch.{RemoteLaunch, RemoteLauncher}
import com.hindog.grid.launch.RemoteLauncher.Argument
import com.hindog.grid.repo.Resource
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION

import scala.collection._

/*
 *    __   _         __
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, /
 *                       /___/
 *
 * TODO: forking for embedded use
 */
abstract class SparkLauncher extends RemoteLauncher[SparkConf] { parent =>

  override type Repr = this.type

  override protected def confGetOption(conf: SparkConf, key: String): Option[String] = conf.getOption(key)
  override protected def confSetIfMissing(conf: SparkConf, key: String, value: String): SparkConf = conf.setIfMissing(key, value)

  flag("--verbose",             "spark.submit.verbose")
  arg("--class",                (instance, _) => RemoteLaunch.launchArgs.mainClass.toOption orElse Option(mainClass.stripSuffix("$")))
  arg("--master",               "spark.master")
  arg("--deploy-mode",          "spark.submit.deployMode")
  arg("--properties-file",      "spark.submit.propertiesFile")
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

  protected def isSubmitted: Boolean = "true" == System.getProperty("SPARK_SUBMIT") || System.getenv("SPARK_YARN_MODE") != null || Option(System.getProperty("spark.app.id")).isDefined || "true" == System.getenv(RemoteLauncher.submitEnvFlag)

  protected def shellCommand: Iterable[String] = RemoteLaunch.submitCommand(Seq("spark-submit"))

  override protected def grid: GridConfig = super.grid.withInheritedSystemPropertiesFilter(_.startsWith("spark."))

  /**
    * Utility method to create managed SparkSession that will:
    *
    *   auto-detect Hive libraries and enable hive-support, if requested
    *   auto-stop any running Spark contexts
    *
    */
  def createSparkSession(conf: SparkConf): SparkSession = {

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

  override def buildProcess(args: Array[String]): ProcessBuilder = {
    val conf = configure(args, new SparkConf(loadDefaults))
    val clusterClasspath = buildClusterClasspath(ClasspathUtils.listCurrentClasspath.map(Resource.url))
    
    if (clusterClasspath.nonEmpty) {
      conf.getOption("spark.master") match {
        case Some("yarn") if !conf.contains("spark.jars") => conf.set("spark.jars", clusterClasspath.map(_.uri).mkString(","))
        case other if !conf.contains("spark.jars") => conf.set("spark.jars", clusterClasspath.map(_.uri).mkString(","))
      }
    }

    val ignoreConfKeys = arguments.flatMap{
      case arg: Argument[_, _] => arg.confKey.toSeq
      case _ => Seq.empty
    }.toSet

    val cmd = (shellCommand ++ arguments.flatMap(_.apply(this, conf)) ++ conf.getAll.filterNot(kv => ignoreConfKeys.contains(kv._1)).flatMap{ case (key, value) => Array("--conf", s"$key=$value") } ++ Seq(applicationJar)).toArray ++ args

    new ProcessBuilder(cmd: _*)
  }


}
