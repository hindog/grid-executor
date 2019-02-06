package com.hindog.grid.hadoop

import com.hindog.grid.GridConfig
import com.hindog.grid.launch.Launcher
import com.hindog.grid.repo.Resource
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration

import scala.collection.Seq

import java.io.File

trait HadoopLauncher extends Launcher[HadoopLauncher.Config] {

  override def isSubmitted: Boolean = {
    val stackMatch = new Throwable().getStackTrace.exists(_.getClassName == "org.apache.hadoop.util.RunJar")
    val envFlag = Option(System.getenv("GRID_SUBMIT")).nonEmpty
    val commandMatch = System.getProperty("sun.java.command").startsWith("org.apache.hadoop.util.RunJar ")
    val propMatch = Option(System.getProperty("hadoop.home.dir")).nonEmpty
    val isSubmitted = envFlag | stackMatch | commandMatch | propMatch

    isSubmitted
  }
  
  override protected[grid] def createLaunchConfig(args: Array[String] = Array.empty): HadoopLauncher.Config = {
    new HadoopLauncher.Config()
      .withMainClass(getClass.getName.stripSuffix("$"))
      .withArgs(args)
      .withShellCommand(Seq("spark-submit"))
      .withConf(_ => new Configuration(true))
  }

}

object HadoopLauncher {

  class Config extends Launcher.Config {

    override type Conf = Configuration

    //  arg("--jt", "mapreduce.jobtracker.address")
    //  arg("--fs", "fs.defaultFS")
    arg("--libjars", "tmpjars")

    override private[grid] var conf = new Configuration(true)

    override protected def getConfValue(key: String): Option[String] = Option(conf.get(key))

    override def buildProcess(): ProcessBuilder = {
      remoteHooks.foreach(_.apply(this))

      import scala.collection.JavaConverters._
      val envConf = HadoopEnvironment.loadConfiguration(true)
      val finalConf = getConf

      val defaultHadoopClasspath = HadoopEnvironment.classpath
      val fullClasspath = defaultHadoopClasspath ++ applicationClasspath
      val jdkJars = FileUtils.listFiles(new File(System.getProperty("java.home")), Array(".jar"), true).asScala.map(_.toURI.toURL.toString).toSet
      val classpathFilter = classpathFilters.foldLeft((cp: Iterable[Resource]) => cp.filter { jar => !jdkJars.contains(jar.uri.toURL.toString) }) {
        (acc, cur) => (cp: Iterable[Resource]) => cur(acc(cp))
      }

      val cp = classpathFilter(applicationClasspath)

      if (cp.nonEmpty && finalConf.get("tmpjars") == null) {
        finalConf.setStrings("tmpjars", cp.map(_.uri.toString).mkString(","))
      }

      val ignoreConfKeys = argumentConfKeys

      // TODO: fix substitutions
      val cmd = shellCommand ++
      Seq("jar", resolveApplicationJar, mainClass) ++
      arguments.flatMap(_.apply()) ++
      finalConf.iterator().asScala.map(me => me.getKey -> me.getValue).filterNot(kv => kv._2.contains("${") || envConf.get(kv._1) == kv._2 || ignoreConfKeys.contains(kv._1)).flatMap { case (key, value) => Array("--conf", s"$key=$value") }

      val builder = new ProcessBuilder(cmd.toSeq: _*)
      builder.environment().put("HADOOP_CLASSPATH", fullClasspath.map(_.toString).mkString(":"))
      builder
    }

    override def gridConfig: GridConfig = super.gridConfig.withInheritedSystemPropertiesFilter(_.startsWith("hadoop."))


  }


}
