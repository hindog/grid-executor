package com.hindog.grid.examples

import com.hindog.grid.hadoop.HDFSRepository
import com.hindog.grid.spark.shell.SparkShellSupport
import com.hindog.grid.{GridConfig, RemoteNodeConfig}

/*
 * NOTE: only tested on IntellJ!  Other IDE's may work in a similar fashion, but that is an exercise left to the reader.
 *
 * This class serves as an example on how to configure an IDE-pimped, remotely-running spark-shell.  This class should not
 * be executed directly, but instead configured as a new "Scala Console" run configuration:
 *
 * 1) Import the `com.hindog.grid:grid-executor-spark2` artifact into your project
 *
 * 2) Copy/paste this example code into your own project and modify the `RemoteNodeConfig` line to reflect your gateway
 *    boxes' IP address.
 *
 * 3) Download AspectJ Weaver jar and place it somewhere in your project folder (ie: `lib/aspectj-weaver-1.8.9.jar`)
 *    https://oss.sonatype.org/content/repositories/releases/org/aspectj/aspectjweaver/1.8.9/aspectjweaver-1.8.9.jar
 *
 * 4) Create a new "Scala Console" run configuration in IntellJ (don't run this class directly) and configure the VM
 *    options as follows:
 *
 *    -Djline.terminal=unix -javaagent:lib/aspectjweaver-1.8.9.jar -Dshell.main.class=com.hindog.grid.examples.SparkShellExample$
 *
 *    .. where the `javaagent` and `shell.main.class` reflect what you add/created in steps 2 and 3.  NOTE: the "$" at
 *    the end of the class name-- this is required to properly find the scala `object` definition for your shell.
 *
 * 5) Run the new configuration!  First time running will take some time to sync all of the jars to the remote machine,
 *    but subsequent runs should enter the shell within seconds.
 *
 * 6) In order for IntellJ to know about the `sc`, `spark` variables and default imports, the shell initialization is
 *    deferred on start and the startup code is printed to the console.  You can then copy/paste the shell init code
 *    into IntelliJ to finish initializing the shell.  (TODO: can we pipe these into IntelliJ somehow on Mac so
 *    it could be automatic?)
 *
 * 7) Profit!  You should be able to access all of your project's classes from within the shell, and submitting actions
 *    to the shell should be reflected in the application master's page served from the cluster.  The shell will behave
 *    the same as if running via a remote shell session, but instead from within your IDE and with full IDE support!
 */
object SparkShellExample extends SparkShellSupport {

  override def master: String = "yarn"
  override def repository = Some(HDFSRepository())
  override def driverVMOptions = "-Dscala.color -Dscala.repl.prompt=\"spark> \""

  conf.set("spark.executor.instances", "3")

  override def grid: GridConfig = GridConfig.apply("spark-shell-example",
    RemoteNodeConfig("10.0.0.25")
      .withInheritedEnv("AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY", "AWS_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"))
      .withStdIn(System.in)
      .withStdOut(System.out)
      .withStdErr(System.err)
      .withStdOutEcho(false)
      .withStdErrEcho(false)
      .withEchoPrefixDisabled

}
