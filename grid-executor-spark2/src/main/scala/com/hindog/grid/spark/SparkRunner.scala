package com.hindog.grid
package spark

import java.io.File
import java.lang.management.ManagementFactory
import java.net.URI

import com.hindog.grid._
import com.hindog.grid.repo.{Repository, Resource, SyncRepositoryHook}
import com.typesafe.scalalogging.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION

import scala.collection.{Iterable, mutable}

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 *
 * TODO: forking for embedded use
 */
trait SparkRunner extends Configurable {

  def master: String
  def deployMode: String = "client"
  def verbose: Boolean = false
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
            val log = Logger(mainClass)

            log.info(s"Running $mainClass remotely under process: ${ManagementFactory.getRuntimeMXBean.getName}")

            val classpath = jarFilter(ClasspathUtils.listCurrentClasspath.flatMap(u => Resource.parse(u.toURI)))
            val jars = classpath.map(cp => repo.flatMap(r => Option(r.resolve(cp))).getOrElse(cp.uri)).map(_.toString).mkString(",")

            // build configuration by folding over all config applicatives
            val conf = config.foldLeft(Config("spark"))((acc, cur) => cur(acc))

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
              .ifThen(conf.getAll.nonEmpty)(_ ++ conf.getAll.flatMap(kv => Array("--conf", s"${kv._1}=${kv._2}"))) :+ System.getProperty("java.class.path").split(File.pathSeparator).head

            val process = new ProcessBuilder(args: _*).inheritIO().start()
            process.waitFor()
          }
        })

        try {
          task.get()
        } catch {
          case ex: Exception => ex.printStackTrace()
        }
        // pause a bit to wait for StdOut/StdErr streams
        Thread.sleep(2000)
      }
    }
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

  def run(args: Array[String]): Unit
}
