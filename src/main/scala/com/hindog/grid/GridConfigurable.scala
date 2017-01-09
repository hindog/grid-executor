package com.hindog.grid

import java.io.{InputStream, PrintStream}
import java.util.Properties

import org.gridkit.nanocloud.RemoteNode
import org.gridkit.vicluster.telecontrol.jvm.JvmProps
import org.gridkit.vicluster.{ViConf, ViNode}

import scala.collection.JavaConverters._
import scala.collection._
import scala.concurrent.duration._

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */

trait GridConfigurable {

	type Repr <: GridConfigurable

	import GridConfigurable._

	def name: String

	// return function that will apply entire configuration to a node
	def config: ViNode => ViNode

	// builder pattern that will apply a single configuration and return 'this' to chain additional configs
	def apply(cfg: ViNode => Unit): Repr

	def addStartupHook(runnable: Hook): Repr
	def addShutdownHook(runnable: Hook): Repr

	def startupHooks: Seq[Hook]
	def shutdownHooks: Seq[Hook]

	def withName(name: String): Repr
	def withSilentShutdown = apply(_.setConfigElement(ViConf.CONSOLE_SILENT_SHUTDOWN, "true"))
	def withStdIn(is: InputStream = System.in) = apply(_.setConfigElement(ViConf.CONSOLE_STD_IN, is))
	def withStdOut(os: PrintStream = System.out) = apply(_.setConfigElement(ViConf.CONSOLE_STD_OUT, os))
	def withConsoleFlush(flush: Boolean) = apply(_.setConfigElement(ViConf.CONSOLE_FLUSH, flush.toString))
	def withStdErr(os: PrintStream = System.err) = apply(_.setConfigElement(ViConf.CONSOLE_STD_ERR, os))
	def withStdOutEcho(enabled: Boolean = true) = apply(_.setConfigElement(ViConf.CONSOLE_STD_OUT_ECHO, enabled.toString))
	def withStdErrEcho(enabled: Boolean = true) = apply(_.setConfigElement(ViConf.CONSOLE_STD_ERR_ECHO, enabled.toString))
	def withEchoPrefix(prefix: String) = apply(_.setConfigElement(ViConf.CONSOLE_ECHO_PREFIX, prefix))
	def withEchoPrefixDisabled = withEchoPrefix(null)

	def withJvmArg(arg: String) = apply(node => JvmProps.addJvmArg(node, arg))
	def withDebugServer(port: Int = 5005, suspend: Boolean = false, timeout: Duration = Duration.Inf) = withJvmArg(s"-agentlib:jdwp=transport=dt_socket,server=y,suspend=${if (suspend) "y" else "n"},address=$port" + (if (timeout != Duration.Inf) s",timeout=${timeout.toMillis}" else ""))
	def withDebugClient(address: String = subnetAddress().map(_.getHostAddress).getOrElse(throw new RuntimeException("Unable to determine bind address. Are you connected to the gravity network?")), port: Int = 5005, suspend: Boolean = false, timeout: Duration = 10 seconds) = withJvmArg(s"-agentlib:jdwp=transport=dt_socket,server=n,address=$address:$port,suspend=${if (suspend) "y" else "n"},timeout=${timeout.toMillis}")
	def withMaxHeap(heap: String) = withJvmArg(s"-Xmx$heap")
	def withMinHeap(heap: String) = withJvmArg(s"-Xms$heap")
	def withAddClasspath(cp: String) = apply(_.setConfigElement(JvmProps.CP_ADD, cp))
	def withRemoveClasspath(cp: String) = apply(_.setConfigElement(JvmProps.CP_REMOVE, cp))
	def withInheritClasspath(value: Boolean = true) = apply(_.setConfigElement(ViConf.CLASSPATH_INHERIT, value.toString))
	def withEnv(name: String, value: String) = apply(node => JvmProps.setEnv(node, name, value))
	def withSystemProperty(key: String, value: String) = apply(_.setProp(key, value))
	def withSystemProperties(props: Properties) = apply(node => {
		props.asScala.foreach(kv => node.setProp(kv._1, kv._2))
	})

	def withPropertyOverrides(props: Properties): Repr = apply(node => {
		val host = Option(node.getProp(RemoteNode.HOST))
		val user = Option(node.getProp(RemoteNode.ACCOUNT)) orElse Option(System.getProperty("user.name"))
		val cloudId = Option(node.getProp(Keys.gridId))

		val scopes = Seq(cloudId, host, user).flatten

		val propMap = {
			// filter+map all "grid.foo.scope1.scope2" props to "foo.scope1.scope2"
			val gridProps = props.entrySet().asScala.map(e => e.getKey.toString -> e.getValue.toString).filter(_._1.startsWith("grid.")).map(kv => kv._1.stripPrefix("grid.") -> kv._2)

			// map props from "foo.scope1.scope2" and  "bar.scope1.scope2" to "scope1.scope2" -> Map("foo" -> "value1", "bar" -> "value2")
			gridProps.map(kv => {
				kv._1.indexOf('.') match {
					case -1 => "" -> kv
					case idx => kv._1.substring(idx + 1) -> (kv._1.substring(0, idx) -> kv._2)
				}
			}).foldLeft(new mutable.HashMap[String, mutable.HashMap[String, String]]())((acc, cur) => {
				acc.getOrElseUpdate(cur._1, new mutable.HashMap[String, String]) += cur._2
				acc
			})
		}

		(0 to scopes.size).flatMap(i => scopes.combinations(i).toList).flatMap(_.permutations).foreach(scope => {
			val key = scope.mkString(".")

			propMap.get(key).filter(_.nonEmpty).foreach(map => {
				map.foreach{ case (k, v) => {
					node.setConfigElement(k, v)
				} }
			})
		})
	})
}

object GridConfigurable {

	implicit class RemoteConfigurable(node: RemoteNodeConfig) {
		def withRemoteJavaExec(cmd: String) = node.apply(_.x(RemoteNode.REMOTE).setRemoteJavaExec(cmd))
		def withRemoteJarCache(path: String) = node.apply(_.x(RemoteNode.REMOTE).setRemoteJarCachePath(path))
	}

	abstract class Hook(val name: String) extends Runnable with Serializable {
		//@transient override implicit lazy val logger: Logger = LoggerFactory.getLogger(classOf[Hook].getName + "." + name)
	}
	
	object Keys {
		val remoteExecutingFlag = "grid.remote.execution"
		val gridId = "grid.id"
	}

}
