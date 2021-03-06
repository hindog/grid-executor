package com.hindog.grid.examples.spark

import com.hindog.grid.spark.SparkLauncher
import com.hindog.grid.spark.shell.SparkShellSupport
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

/*
 * NOTE: only tested on IntellJ!  Other IDE's may work in a similar fashion, but that is an exercise left to the reader.
 *
 * This class serves as an example on how to configure an IDE-pimped, remotely-running spark-shell.  This class should not
 * be executed directly, but instead configured as a new "Scala Console" run configuration:
 *
 * 1) Import the `com.hindog.grid:grid-executor-spark2` artifact into your project
 *
 * 2) Download AspectJ Weaver jar and place it somewhere in your project folder (ie: `lib/aspectj-weaver-1.8.9.jar`)
 *    https://oss.sonatype.org/content/repositories/releases/org/aspectj/aspectjweaver/1.8.9/aspectjweaver-1.8.9.jar
 *
 * 3) Create a new "Scala Console" run configuration in IntellJ (don't run this class directly) and configure the VM
 *    options as follows:
 *
 *    -Djline.terminal=unix -javaagent:lib/aspectjweaver-1.8.9.jar -Dshell.main.class=com.hindog.grid.examples.SparkShellExample$
 *
 *    .. where the `javaagent` and `shell.main.class` reflect what you add/created in steps 2 and 3.  NOTE: the "$" at
 *    the end of the class name-- this is required to properly find the scala `object` definition for your shell.
 *
 * 4) Run the new configuration!  First time running will take some time to sync all of the jars to the remote machine,
 *    but subsequent runs should enter the shell within seconds.
 *
 * 5) In order for IntellJ to know about the `sc`, `spark` variables and default imports, the shell initialization is
 *    deferred on start and the startup code is printed to the console.  You can then copy/paste the shell init code
 *    into IntelliJ to finish initializing the shell.  (TODO: can we pipe these into IntelliJ somehow on Mac so
 *    it could be automatic?)
 *
 * 6) Profit!  You should be able to access all of your project's classes from within the shell, and submitting actions
 *    to the shell should be reflected in the application master's page served from the cluster.  The shell will behave
 *    the same as if running via a remote shell session, but instead from within your IDE and with full IDE support!
 */
object SparkShellExample extends SparkShellSupport {

  override protected def configureLaunch(config: SparkLauncher.Config): SparkLauncher.Config = {
    super.configureLaunch(config).withGridConfig(_
      .withStdIn(System.in)
      .withStdOut(System.out)
      .withStdErr(System.err)
      .withStdOutEcho(false)
      .withStdErrEcho(false)
      .withEchoPrefixDisabled
    )
  }

  // this will never get reached because of how SparkShellSupport is implemented
  override def run(args: Array[String])(implicit spark: SparkSession, sc: SparkContext): Unit = ???
}
