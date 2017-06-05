package com.hindog.grid.spark.shell

import java.io.File

import com.hindog.grid.ClasspathUtils
import com.hindog.grid.repo.Resource
import com.hindog.grid.spark.SparkRunner
import org.apache.spark.repl.Main.outputDir

import scala.tools.nsc.GenericRunnerSettings
import scala.tools.nsc.interpreter.ILoop
import scala.util.Properties.{javaVersion, javaVmName, versionString}

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
abstract class SparkShellRunner extends SparkRunner { outer =>

  conf(_.append("spark.driver.extraJavaOptions", "-Dscala.repl.prompt=\"spark> \""))

  override def run(args: Array[String]): Unit = {

    val classPath = jarFilter(ClasspathUtils.listCurrentClasspath.flatMap(u => Resource.parse(u.toURI))).map(_.uri.toString).mkString(File.pathSeparator)
    val iloop = new ILoop() {
      override def printWelcome(): Unit = outer.printWelcome()
    }

    System.setProperty("spark.repl.class.outputDir", outputDir.getAbsolutePath())

    val interpArguments = List(
      "-Yrepl-class-based",
      "-Yrepl-outdir", s"${outputDir.getAbsolutePath}",
      "-classpath", classPath,
      "-usejavacp"
    ) ++ args.toList

    val settings = new GenericRunnerSettings(err => Console.err.println(err))
    settings.processArguments(interpArguments, true)

    iloop.process(settings) // Repl starts and goes in loop of R.E.P.L

  }

  def initCommands(): String = {
    s"""
       | @transient val spark = ${getClass.getName.stripSuffix("$")}.createSparkSession
       | @transient val sc = spark.sparkContext
       | import spark.implicits._, spark._, org.apache.spark.SparkContext._, org.apache.spark.sql.functions._
    """.stripMargin('|')
  }

  /** Print a welcome message */
  def printWelcome() {
    import org.apache.spark.SPARK_VERSION

    println("""Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version %s
      /_/
         """.format(SPARK_VERSION))
    val welcomeMsg = "Using Scala %s (%s, Java %s)".format(
      versionString, javaVmName, javaVersion)
    println(welcomeMsg)
    println("Type in expressions to have them evaluated.")
    println("Type :help for more information.")
    println("")
    println(" !! Shell initialization deferred for IDE use")
    println(" !! Please copy/paste the following to initialize the shell for IDE use:")
    println("")
    println(initCommands())
  }

}
