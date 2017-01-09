package com.hindog.grid

import com.hindog.grid.GridConfigurable.Hook
import org.gridkit.nanocloud.{Cloud, RemoteNode, VX}
import org.gridkit.vicluster.{ViNode, ViProps}

import scala.collection._

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

case class RemoteNodeConfig private (hostname: String, name: String, config: ViNode => ViNode, startupHooks: Seq[Hook], shutdownHooks: Seq[Hook]) extends NodeConfig {

	override type Repr = RemoteNodeConfig

	override def create(cloud: Cloud): ViNode = {
		val node = cloud.node(name)
		ViProps.at(node).setRemoteType()
		RemoteNode.at(node).useSimpleRemoting()
		RemoteNode.at(node).setRemoteHost(hostname)
		RemoteNode.at(node).setRemoteJarCachePath(".jar-cache")
		node
	}

	override def withName(name: String): RemoteNodeConfig = copy(name = name)
	override def apply(configStmt: ViNode => Unit): RemoteNodeConfig = copy(config = node => { configStmt(config(node)); node })

	def addStartupHook(hook: Hook): RemoteNodeConfig = copy(startupHooks = startupHooks :+ hook)
	def addShutdownHook(hook: Hook): RemoteNodeConfig = copy(shutdownHooks = shutdownHooks :+ hook)
}

object RemoteNodeConfig {
	def apply(hostname: String, name: String, startupHooks: Seq[Hook] = Seq.empty, shutdownHooks: Seq[Hook] = Seq.empty): RemoteNodeConfig = {
		RemoteNodeConfig(hostname, name, identity, startupHooks, shutdownHooks)
	}

	def apply(hostname: String): RemoteNodeConfig = RemoteNodeConfig(hostname, hostname)
}

case class LocalNodeConfig private (name: String, config: ViNode => ViNode, startupHooks: Seq[Hook], shutdownHooks: Seq[Hook]) extends NodeConfig {

	override type Repr = LocalNodeConfig

	override def create(cloud: Cloud): ViNode = {
		val node = cloud.node(name)
		node.x(VX.TYPE).setLocal()
		node
	}

	override def withName(name: String): LocalNodeConfig = copy(name = name)
	override def apply(configStmt: ViNode => Unit): LocalNodeConfig = copy(config = node => { configStmt(config(node)); node })

	def addStartupHook(hook: Hook): LocalNodeConfig = copy(startupHooks = startupHooks :+ hook)
	def addShutdownHook(hook: Hook): LocalNodeConfig = copy(shutdownHooks = shutdownHooks :+ hook)

}

object LocalNodeConfig {
	def apply(name: String, startupHooks: Seq[Hook] = Seq.empty, shutdownHooks: Seq[Hook] = Seq.empty): LocalNodeConfig = {
		LocalNodeConfig(name, identity, startupHooks, shutdownHooks)
	}
}

