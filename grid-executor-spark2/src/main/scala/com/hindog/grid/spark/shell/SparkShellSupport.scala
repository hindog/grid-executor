package com.hindog.grid.spark.shell

import java.io.File
import com.hindog.grid.ClasspathUtils
import com.hindog.grid.repo.Resource
import com.hindog.grid.spark._
import org.apache.spark.SparkConf
import org.apache.spark.repl.Main.outputDir

import scala.collection._
import scala.tools.nsc.GenericRunnerSettings
import scala.tools.nsc.interpreter.ILoop
import scala.util.Properties.{javaVersion, javaVmName, versionString}

import java.net.InetAddress

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
trait SparkShellSupport extends SparkLauncher {
  
  def shellColor: Boolean = false
  def prompt: String = "spark> "

  def initCommands(): String = {
    s"""
       | @transient val spark = ${getClass.getName.stripSuffix("$")}.createSparkSession
       | @transient val sc = spark.sparkContext
       | import spark.implicits._, spark._, org.apache.spark.SparkContext._, org.apache.spark.sql.functions._
    """.stripMargin('|')
  }

  override def run(args: Array[String]): Unit = {
    val conf = new SparkConf(true)

    conf.set("spark.repl.classpath", ClasspathUtils.listCurrentClasspath.map(u => Resource.uri(u.toURI)).map(_.uri.toString).mkString(File.pathSeparator))
    conf.set("spark.repl.class.outputDir", outputDir.getAbsolutePath)

    if (shellColor) {
      System.setProperty("scala.color", "true")
      conf.append("spark.driver.extraJavaOptions", s"-Dscala.color=true")
    }

    System.setProperty("scala.repl.prompt", prompt)
    conf.append("spark.driver.extraJavaOptions", "-Dscala.repl.prompt=\"" + prompt + "\"")
    conf.set("spark.submit.deployMode", "client")

    val interpArguments = List(
      "-Yrepl-class-based",
      "-Yrepl-outdir", s"${outputDir.getAbsolutePath}",
      "-classpath", Option(conf.get("spark.repl.classpath")).getOrElse(System.getProperty("java.class.path")),
      "-usejavacp"
    ) ++ args.toList

    val settings = new GenericRunnerSettings(err => Console.err.println(err))
    settings.processArguments(interpArguments, true)

    iloop.process(settings) // Repl starts and goes in loop of R.E.P.L

  }


  @transient lazy val iloop = new ILoop() {
    /** Print a welcome message */
    override def printWelcome() {
      import org.apache.spark.SPARK_VERSION

      echo("""Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version %s
      /_/
         """.format(SPARK_VERSION))
      val welcomeMsg = "Using Scala %s (%s, Java %s)".format(
        versionString, javaVmName, javaVersion)
      echo(welcomeMsg)
      echo("Type in expressions to have them evaluated.")
      echo("Type :help for more information.")
      echo("")
      echo(s" Host: ${InetAddress.getLocalHost.getHostName}")
      echo("")
      echo(" !! Shell initialization deferred for IDE use")
      echo(" !! Please copy/paste the following to initialize the shell for IDE use:")
      echo("")
      echo(initCommands())
    }

    /** Add repl commands that needs to be blocked. e.g. reset */
    @transient lazy private val blockedCommands = Set[String]()

    /** Standard commands */
    @transient lazy val sparkStandardCommands: List[LoopCommand] =
    standardCommands.filter(cmd => !blockedCommands(cmd.name))

    /** Available commands */
    override def commands: List[LoopCommand] = sparkStandardCommands

    //  /**
    //    * We override `loadFiles` because we need to initialize Spark *before* the REPL
    //    * sees any files, so that the Spark context is visible in those files. This is a bit of a
    //    * hack, but there isn't another hook available to us at this point.
    //    */
    //  override def loadFiles(settings: Settings): Unit = {
    //    initCommands()
    //    super.loadFiles(settings)
    //  }

    override def resetCommand(line: String): Unit = {
      super.resetCommand(line)
      initCommands()
      echo("Note that after :reset, state of SparkSession and SparkContext is unchanged.")
    }

  }

}
