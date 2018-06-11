package com.hindog.grid.launch

import org.rogach.scallop.ScallopConf

import java.io.File
import java.util.Properties

class RemoteLaunchArgs(args: Array[String], props: Properties) extends ScallopConf(args) {

  val help          = opt[String](name = "help", 'h', required = false, descr = "Print help")
  val javaExec      = opt[String](name = "java-exec", 'j', required = false, descr = "Path to java executable to use", default = Option(props.getProperty("grid.java-exec")).filter(_.nonEmpty))
  val identityKey   = opt[File](name = "identity-file", 'i', required = false, descr = "SSH identity file to use", default = Option(props.getProperty("grid.identity-file")).filter(_.nonEmpty).map(f => new File(f)))
  val remoteAccount = opt[String](name = "remote-account", 'u', required = false, descr = "SSH remote account username", default = Option(props.getProperty("grid.remote-account")) orElse Option(System.getProperty("user.name")).filter(_.nonEmpty))
  val env           = props[String](name = 'E', descr = "List of environment variables to set (eg: -EVAR1=abc VAR2=def)")
  val sysProps      = props[String](name = 'D', descr = "List of system properties to set (eg: -Dprop1=value1 prop2=value2)")

  val node          = opt[String](name = "node", short = 'n', required = false, descr = "Remote node to launch on", default = Option(props.getProperty("grid.node")).filter(_.nonEmpty))
  val submitCommand = opt[List[String]]("submit-command", required = false, noshort = true, descr = "Submit command", default = Option(props.getProperty("grid.submit-command")).filter(_.nonEmpty).map(c => List(c)))
  val mainClass     = opt[String]("class", required = false, descr = "Class name to launch (instance of SparkRunner)" , default = Option(props.getProperty("grid.main-class")).filter(_.nonEmpty))
  val loadDefaults  = opt[Boolean]("load-defaults", required = false, descr = "Load default conf", default = Option(props.getProperty("grid.load-defaults")).map(_.toBoolean) orElse Some(true))
  val jarCacheRepositoryClass = opt[String]("jar-cache-repository-class", required = false, descr = "Jar cache repository implementation class to sync jars to prior to launch", default = Option(props.getProperty("grid.jar-cache-repository-class")).filter(_.nonEmpty))

  val conf          = propsLong[String]("conf", descr = "List of conf values to pass to submit command")
  val programArgs   = opt[List[String]]("args", descr = "List of arguments to pass to program")

  verify()
}
