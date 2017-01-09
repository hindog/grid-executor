package com.hindog.grid.spark.shell

import java.io.File

import com.hindog.grid.spark.SparkRunner
import org.apache.spark.repl.SparkILoop

import scala.collection._
import scala.tools.nsc.GenericRunnerSettings

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
trait SparkShellSupport extends SparkILoop with SparkRunner {
  override def deployMode = "client"

  import org.apache.spark.repl.Main._

  sparkConf.set("spark.ui.enabled", "false")

  override def prompt: String = "spark> "

  override def run(args: Array[String]): Unit = {

    val jars = getUserJars.mkString(File.pathSeparator)
    val interpArguments = List(
      "-Yrepl-class-based",
      "-Yrepl-outdir", s"${outputDir.getAbsolutePath}",
      "-classpath", jars,
      "-usejavacp",
      "-Dscala.color"
    ) ++ args.toList

    val settings = new GenericRunnerSettings(err => Console.err.println(err))
    settings.processArguments(interpArguments, true)

    sparkConf.getAll.foreach(kv => conf.set(kv._1, kv._2))

    this.process(settings) // Repl starts and goes in loop of R.E.P.L
    Option(sparkContext).map(_.stop)
  }

  override def initializeSpark(): Unit = {}

  override def printWelcome(): Unit = {
    super.printWelcome()
    println()
    println("// !! shell initialization deferred for IDE entry !! " +
            "\n// please copy/paste the initialization code below to continue to initialize this shell: ")
    println()
    println("\t@transient val sc = org.apache.spark.repl.Main.createSparkSession()")
    println("\timport sc.implicits._, sc._, org.apache.spark.SparkContext._, org.apache.spark.sql.functions._")
    println()
  }

  protected def getUserJars: Seq[String] = {
    val sparkJars = conf.getOption("spark.jars")
    if (conf.get("spark.master") == "yarn") {
      val yarnJars = conf.getOption("spark.yarn.dist.jars")
      unionFileLists(sparkJars, yarnJars).toSeq
    } else {
      sparkJars.map(_.split(",")).map(_.filter(_.nonEmpty)).toSeq.flatten
    }
  }

  protected def unionFileLists(leftList: Option[String], rightList: Option[String]): Set[String] = {
    var allFiles = Set[String]()
    leftList.foreach { value => allFiles ++= value.split(",") }
    rightList.foreach { value => allFiles ++= value.split(",") }
    allFiles.filter { _.nonEmpty }
  }
}
