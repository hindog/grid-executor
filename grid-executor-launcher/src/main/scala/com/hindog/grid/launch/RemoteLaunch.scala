package com.hindog.grid.launch

import com.hindog.grid._

object RemoteLaunch {

  private val launchPropsPrefix = getClass.getName + ".args"

  def main(args: Array[String]): Unit = {

    lazy val launchArgs: RemoteLaunchArgs = new RemoteLaunchArgs(args, System.getProperties)

    val mainClass = launchArgs.mainClass()

    // encode the launch args as system properties so they are available globally to simplify method signatures
    val mainMethod = Class.forName(mainClass).getMethod("main", classOf[Array[String]])
    System.setProperty(launchPropsPrefix + ".length", args.length.toString)
    args.indices.foreach(i => System.setProperty(launchPropsPrefix + s".$i", args(i)))

    mainMethod.invoke(null, launchArgs.programArgs.toOption.getOrElse(List.empty).toArray)
  }

  @transient lazy val launchArgs: RemoteLaunchArgs = {
    val args: Array[String] = Option(System.getProperty(s"$launchPropsPrefix.length")).filter(_ != "0") match {
      case None => Array.empty
      case Some(len) => (0 until len.toInt).flatMap(i => Option(System.getProperty(s"$launchPropsPrefix.$i"))).toArray
    }

    new RemoteLaunchArgs(args, System.getProperties)
  }

  def submitCommand(default: => Iterable[String]): Iterable[String] = launchArgs.submitCommand.toOption.getOrElse(default)

  def gridConfig(name: String): GridConfig = {
    val nc = launchArgs.node.toOption match {
      case None => throw new RuntimeException("No node configured for launch!  Use -Dgrid.node=<hostname> or -n <hostname>")
      case Some("local") => LocalNodeConfig(s"local")
      case Some(remote) => RemoteNodeConfig(remote)
        .ifThen(launchArgs.remoteAccount.isDefined)(_.withSSHAccount(launchArgs.remoteAccount()))
        .ifThen(launchArgs.identityKey.isDefined)(_.withSSHKey(launchArgs.identityKey()))
        .ifThen(launchArgs.javaExec.isDefined)(_.withJavaCommand(launchArgs.javaExec()))
    }

    GridConfig(name, nc)
      .withInheritedSystemPropertiesFilter(_.startsWith(RemoteLaunch.launchPropsPrefix))
      .withSystemProperties(launchArgs.sysProps.toSeq: _*)
      .withEnv(launchArgs.env.toSeq: _*)
  }
}
