package com.hindog.grid.examples

import java.io.File
import java.net.URI

import com.hindog.grid.hadoop.HDFSRepository
import com.hindog.grid.spark.shell.SparkShellSupport
import com.hindog.grid.{GridConfig, RemoteNodeConfig}

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
object SparkShellExample extends SparkShellSupport {
  override def master: String = "yarn"
  override def repository = Some(HDFSRepository())
  override def driverVMOptions = "-Dscala.color -Dscala.repl.prompt=\"spark> \""

  conf.set("spark.executor.instances", "3")

  override def assemblyArchive: Option[URI] = Some(new URI("hdfs:/user/spark/share/lib/spark-assembly-2.1.0.zip"))

  override def grid: GridConfig = GridConfig.apply("spark-shell-example",
    RemoteNodeConfig("10.0.2.221")
      .withSSHAccount("hadoop")
      .withSSHKey(new File("~/.ssh/devKeyPair.pem"))
      .withInheritedEnv("AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY", "AWS_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"))
      .withStdIn(System.in)
      .withStdOut(System.out)
      .withStdErr(System.err)
      .withStdOutEcho(false)
      .withStdErrEcho(false)
      .withEchoPrefixDisabled
      
}
