package com.hindog.grid
package spark

import com.hindog.grid.launch._
import com.hindog.grid.repo.{Repository, Resource}
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION

import java.io.File

import scala.collection._

/*
 *    __   _         __
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, /
 *                       /___/
 *
 */
abstract class SparkLauncher extends Launcher[SparkLauncher.Config] { parent =>

  /**
    * Will try to detect if we have been submitted via spark-submit (vs running as a
    * regular main() application.  If yes, we will not invoke the remote launch but
    * will instead proceed to run the job.
    */
  override def isSubmitted: Boolean = {
    val stack = new Throwable().fillInStackTrace()
    stack.getStackTrace.exists(_.getClassName.startsWith("org.apache.spark.deploy"))
  }

  override protected[grid] def createLaunchConfig(args: Array[String] = Array.empty): SparkLauncher.Config = {
    configureLaunch(new SparkLauncher.Config()
      .withMainClass(getClass.getName.stripSuffix("$"))
      .withArgs(args)
      .withShellCommand(Seq("spark-submit")))
  }

  /**
    * Create SparkConf.
    */
  protected def createSparkConf(args: Array[String], default: SparkConf): SparkConf = new SparkConf(true)

  /**
    * Utility method to create managed SparkSession that will:
    *
    *   auto-detect Hive libraries and enable hive-support, if requested
    *   auto-stop any running Spark contexts
    *
    */
  protected def createSparkSession(conf: SparkConf): SparkSession = {

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
      if (SparkLauncher.hiveClassesArePresent) {
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

  /**
    * "Launch" method that replaces the standard `main` method and, depending
    * on the launch config, will run via one of these methods:
    *
    * (a) remotely, on another machine
    * (b) locally, in another process
    * (c) locally, in same process
    *
    * ... and will initialize the SparkSession/SparkContext and delegate to `run`.
    *
    * If this class is executed via "spark-submit", then (c) will be used, otherwise,
    * (a) or (b) will be used, depending on the launch config (LocalNodeConfig or RemoteNodeConfig).
    */

  def launch(args: Array[String]): Unit = {
    // Create initial SparkConf and pass to [[configure]] method.
    // SparkApp expects [[configure]] to initialize the member variable 'conf' with the SparkConf to use
    val conf = createSparkConf(args, new SparkConf(true))
    val spark: SparkSession = createSparkSession(conf)
    try {
      run(args)(spark, spark.sparkContext)
    } finally spark.close()
  }

  /**
    * Spark application entry-point
    */
  def run(args: Array[String])(implicit spark: SparkSession, sc: SparkContext): Unit

}

object SparkLauncher {

  protected def hiveClassesArePresent: Boolean = {
    try {
      Class.forName("org.apache.hadoop.hive.conf.HiveConf")
      true
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError => false
    }
  }


  class Config extends Launcher.Config with Logging {

    override type Conf = SparkConf
    
    override private[grid] var conf: Conf = new SparkConf(true)

    override protected def getConfValue(key: String): Option[String] = conf.getOption(key)

    flag("--verbose",             "spark.submit.verbose")
    arg("--class",                () => Option(mainClass.stripSuffix("$")))
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

    override def buildProcess(): ProcessBuilder = {
      remoteHooks.foreach(_.apply(this))

      val finalConf = getConf

      // override any SparkConf settings that are set using system properties
      sys.props.toList.filter(_._1.startsWith("spark.")).foreach { case (k, v) => finalConf.set(k, v) }

      import scala.collection.JavaConverters._

      val jdkJars = FileUtils.listFiles(new File(System.getProperty("java.home")), Array(".jar"), true).asScala.map(_.toURI.toURL.toString).toSet
      val classpathFilter = classpathFilters.foldLeft((cp: Iterable[Resource]) => cp.filter { jar => !jdkJars.contains(jar.uri.toURL.toString) }) {
        (acc, cur) => (cp: Iterable[Resource]) => cur(acc(cp))
      }

      val filteredClasspath = classpathFilter(applicationClasspath)

      val cp = repository match {
        case Some(repo) => {
          // filter for cluster classpath
          val clusterClasspathFilter = clusterClasspathFilters.foldLeft(identity[Iterable[Resource]] _) {
            (acc, cur) => (cp: Iterable[Resource]) => cur(acc(cp))
          }

          // sync local jars to repository and return the repository URLs
          clusterClasspathFilter(filteredClasspath.map(c => repo.put(c)))
        }
        case None => {
          // Yarn will add local jars to cluster automatically, but other cluster managers will not and may not work with local jars, so
          // we log a warning if we not running Yarn here.
          if (!finalConf.getOption("spark.master").contains("yarn")) {
            logger.warn("Application running in 'cluster' mode and no jar repository specified. Using local jars which may not be compatible with this Spark cluster.")
          }
          // For local jars, we will sync to a local temporary repository because it will ensure that all filenames are unique.
          // Otherwise, we could have issues with duplicate paths/files (eg: "scala-2.11" output folder in a multi-module build)
          // and Spark will log an error and fail to add the duplicate path.
          val tmp = Repository.localTemp()
          filteredClasspath.map(c => tmp.put(c))
        }
      }

      if (cp.nonEmpty) {
        if (!finalConf.contains("spark.jars")) {
          finalConf.set("spark.jars", cp.map(_.uri).mkString(","))
        }
      }

      val ignoreConfKeys = argumentConfKeys

      val cmd = (shellCommand ++ arguments.flatMap(_.apply()) ++ finalConf.getAll.filterNot(kv => ignoreConfKeys.contains(kv._1)).flatMap{ case (key, value) => Array("--conf", s"$key=$value") } ++ Seq(resolveApplicationJar)).toArray ++ args
      new ProcessBuilder(cmd: _*)
    }

    override def gridConfig: GridConfig = super.gridConfig.withInheritedSystemPropertiesFilter(_.startsWith("spark."))
  }

}
