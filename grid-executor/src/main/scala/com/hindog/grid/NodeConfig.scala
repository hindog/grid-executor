package com.hindog.grid

import org.gridkit.nanocloud._
import org.gridkit.vicluster._

import scala.collection._

import java.io.File

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */

trait NodeConfig extends GridConfigurable {

	override type Repr <: NodeConfig

	// create a node with empty config
	def create(cloud: Cloud): ViNode
	def name: String
}

case class RemoteNodeConfig private (hostname: String, name: String, slots: Option[Int], config: ViNode => ViNode, startupHooks: Seq[Hook], shutdownHooks: Seq[Hook], sshKey: Option[File], sshAccount: Option[String], javaCommand: Option[String], jarCache: File) extends NodeConfig {

	override type Repr = RemoteNodeConfig

	override def create(cloud: Cloud): ViNode = {
		val node = cloud.node(name)
		ViProps.at(node).setRemoteType()
		RemoteNode.at(node).useSimpleRemoting()
		RemoteNode.at(node).setRemoteHost(hostname)
		RemoteNode.at(node).setRemoteJarCachePath(jarCache.toString)

		javaCommand.foreach(j => RemoteNode.at(node).setRemoteJavaExec(j.toString))
		sshAccount.foreach(a => RemoteNode.at(node).setRemoteAccount(a))
		sshKey.foreach(f => RemoteNode.at(node).setSshPrivateKey(f.toString))
		node
	}

	override def withName(name: String): RemoteNodeConfig = copy(name = name)
	override def withSlots(slots: Int): RemoteNodeConfig = copy(slots = Option(slots))
	override def apply(configStmt: ViNode => Unit): RemoteNodeConfig = copy(config = node => { configStmt(config(node)); node })

	def addStartupHook(hook: Hook): RemoteNodeConfig = copy(startupHooks = startupHooks :+ hook)
	def addShutdownHook(hook: Hook): RemoteNodeConfig = copy(shutdownHooks = shutdownHooks :+ hook)
	def withJarCache(jarCache: File): RemoteNodeConfig = copy(jarCache = jarCache)
	def withSSHAccount(account: String): RemoteNodeConfig = copy(sshAccount = Some(account))
	def withSSHKey(sshKey: File): RemoteNodeConfig = copy(sshKey = Some(sshKey))
	def withJavaCommand(cmd: String): RemoteNodeConfig = copy(javaCommand = Some(cmd))
}

object RemoteNodeConfig {
	def apply(hostname: String, name: String, startupHooks: Seq[Hook] = Seq.empty, shutdownHooks: Seq[Hook] = Seq.empty, sshKey: Option[File] = None, sshAccount: Option[String] = None, javaCommand: Option[String] = None, jarCache: File = new File(".jar-cache")): RemoteNodeConfig = {
		RemoteNodeConfig(hostname, name, None, identity, startupHooks, shutdownHooks, sshKey, sshAccount, javaCommand, jarCache)
	}

	def apply(hostname: String): RemoteNodeConfig = RemoteNodeConfig(hostname, hostname)
}

case class LocalNodeConfig private (name: String, slots: Option[Int], config: ViNode => ViNode, startupHooks: Seq[Hook], shutdownHooks: Seq[Hook]) extends NodeConfig {

	override type Repr = LocalNodeConfig

	override def create(cloud: Cloud): ViNode = {
		val node = cloud.node(name)
		node.x(VX.TYPE).setLocal()
		node
	}

	override def withName(name: String): LocalNodeConfig = copy(name = name)
	override def withSlots(slots: Int): LocalNodeConfig = copy(slots = Option(slots))

	override def apply(configStmt: ViNode => Unit): LocalNodeConfig = copy(config = node => { configStmt(config(node)); node })

	def addStartupHook(hook: Hook): LocalNodeConfig = copy(startupHooks = startupHooks :+ hook)
	def addShutdownHook(hook: Hook): LocalNodeConfig = copy(shutdownHooks = shutdownHooks :+ hook)

}

object LocalNodeConfig {
	def apply(name: String, startupHooks: Seq[Hook] = Seq.empty, shutdownHooks: Seq[Hook] = Seq.empty): LocalNodeConfig = {
		LocalNodeConfig(name, None, identity, startupHooks, shutdownHooks)
	}
}

