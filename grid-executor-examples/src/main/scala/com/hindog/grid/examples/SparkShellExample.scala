package com.hindog.grid.examples

import java.io.File

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

  override def repository = Some(HDFSRepository())

  override def grid: GridConfig = GridConfig.apply("spark-emr-example",
    RemoteNodeConfig("10.0.2.25")
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
