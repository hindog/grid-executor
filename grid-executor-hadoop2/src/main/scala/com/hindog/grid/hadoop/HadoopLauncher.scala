package com.hindog.grid.hadoop

import com.hindog.grid.{ClasspathUtils, GridConfig}
import com.hindog.grid.launch.RemoteLauncher
import com.hindog.grid.launch.RemoteLauncher.Argument
import org.apache.hadoop.conf.Configuration

import scala.collection.Seq

import java.net.URI
import java.nio.file.Paths

trait HadoopLauncher extends RemoteLauncher[Configuration] {

  override type Repr = this.type

  override protected def confGetOption(conf: Configuration, key: String): Option[String] = Option(conf.get(key))
  
  override protected def confSetIfMissing(conf: Configuration, key: String, value: String): Configuration = { if (conf.get(key) == null) conf.set(key, value); conf }

  override protected def shellCommand: Iterable[String] = Iterable("hadoop")

  override protected def grid: GridConfig = super.grid.withInheritedSystemPropertiesFilter(_.startsWith("hadoop."))
  
  override protected def isSubmitted: Boolean = {

    val stackMatch = new Throwable().getStackTrace.exists(_.getClassName == "org.apache.hadoop.util.RunJar")
    val envFlag = Option(System.getenv("GRID_SUBMIT")).nonEmpty
    val commandMatch = System.getProperty("sun.java.command").startsWith("org.apache.hadoop.util.RunJar ")
    val propMatch = Option(System.getProperty("hadoop.home.dir")).nonEmpty
    val isSubmitted = envFlag | stackMatch | commandMatch | propMatch

    isSubmitted
  }

//  arg("--jt", "mapreduce.jobtracker.address")
//  arg("--fs", "fs.defaultFS")
  arg("--libjars", "tmpjars")

  /**
    * Build remote submit command
    */
  override def buildProcess(args: Array[String]): ProcessBuilder = {
    import scala.collection.JavaConverters._

    val envConf = HadoopEnvironment.loadConfiguration()
    val defaultHadoopClasspath = HadoopEnvironment.classpath
    val fullClasspath = (defaultHadoopClasspath ++ ClasspathUtils.listCurrentClasspath).map(url => Paths.get(url.toURI))

    val conf = configure(args, envConf)

    if (clusterClasspath.nonEmpty && conf.get("tmpjars") == null) {
      //conf.setStrings("tmpjars", clusterClasspath.map(cp => new URI(cp)).filter(uri => uri.getScheme != null && uri.getScheme != "file").map(_.toString).toSeq: _*)
    }

    val ignoreConfKeys = arguments.flatMap{
      case arg: Argument[_, _] => arg.confKey.toSeq
      case _ => Seq.empty
    }.toSet

    // TODO: fix substitutions
    val cmd = shellCommand ++
      Seq("jar", applicationJar, mainClass) ++
      arguments.flatMap(_.apply(this, conf)) ++
      conf.iterator().asScala.map(me => me.getKey -> me.getValue).filterNot(kv => kv._2.contains("${") || envConf.get(kv._1) == kv._2 || ignoreConfKeys.contains(kv._1)).flatMap { case (key, value) => Array("--conf", s"$key=$value") }

    val builder = new ProcessBuilder(cmd.toSeq: _*)
    builder.environment().put("HADOOP_CLASSPATH", fullClasspath.map(_.toString).mkString(":"))
    builder
  }
}
