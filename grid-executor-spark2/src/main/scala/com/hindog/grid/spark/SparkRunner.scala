package com.hindog.grid
package spark

import java.lang.management.ManagementFactory

import com.hindog.grid._
import com.hindog.grid.repo.{Repository, Resource, SyncRepositoryHook}
import com.typesafe.scalalogging.Logger
import org.apache.spark.SparkConf
import org.gridkit.vicluster.telecontrol.ClasspathUtils

import scala.collection.JavaConversions._

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

  @transient lazy val sparkConf = new SparkConf()

  def deployMode: String = "cluster"
  def verbose: Boolean = true
  def master: String = "yarn"
  def proxyUser: String = ""

  def driverVMOptions: String = ""
  def driverMemory: String = ""
  def driverLibraryPath: String = ""
  def driverClasspath: String = ""

  def jarFilter: Iterable[Resource] => Iterable[Resource] = identity
  
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

            val classpath = jarFilter(ClasspathUtils.listCurrentClasspath().flatMap(u => Resource.parse(u.toURI)))
            val jars = classpath.map(cp => repo.flatMap(r => Option(r.resolve(cp))).getOrElse(cp.uri)).map(_.toString).mkString(",")
            val args = Array(
              "/bin/bash", "spark-submit",
              "--master", master,
              "--deploy-mode", deployMode,
              "--class", mainClass,
              "--jars", jars
            ).ifThen(verbose)(_ :+ "--verbose")
             .ifThen(driverClasspath.nonEmpty)(_ ++ Array("--driver-class-path", driverClasspath))
             .ifThen(driverMemory.nonEmpty)(_ ++ Array("--driver-memory", driverMemory))
             .ifThen(driverLibraryPath.nonEmpty)(_ ++ Array("--driver-library-path", driverLibraryPath))
             .ifThen(driverVMOptions.nonEmpty)(_ ++ Array("--driver-java-options", driverVMOptions)) :+ System.getProperty("java.class.path").split(":").head

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