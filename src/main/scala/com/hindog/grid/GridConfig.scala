package com.hindog.grid

import java.util.concurrent.{Future => _}

import com.hindog.grid.GridConfigurable.Hook
import org.gridkit.vicluster._

import scala.collection._
import scala.util.Random

/*
 *    __   _         __
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, /
 *                       /___/
 *
 * This class defines a configuration to be used with GridExecutor
 *
 * You can add a node multiple times to a config, and for each one you get an "execution slot" on that node, ie:
 * a grid config that contains nodes (server1, server1, server2, server3, server3, server3) means that
 * server1 will have 2 "execution slots" available
 * server2 will have 1 "execution slots" available
 * server3 will have 3 "execution slots" available
 *
 * TODO: allow for a custom strategy to select which node/slot to execute on for each slot
 */

case class GridConfig(name: String, nodes: Seq[NodeConfig], config: ViNode => ViNode = identity, startupHooks: Seq[Hook] = Seq.empty, shutdownHooks: Seq[Hook] = Seq.empty) extends GridConfigurable {

	override type Repr = GridConfig

	override def apply(configStmt: ViNode => Unit): GridConfig = copy(config = node => { configStmt(config(node)); node })

	// override name to allow sourcing from different set of props
	def withName(name: String): GridConfig = copy(name = name)
	def withConfig(configure: ViNode => ViNode): GridConfig = copy(config = node => configure(config(node)))
	def addStartupHook(hook: Hook): GridConfig = copy(startupHooks = startupHooks :+ hook)
	def addShutdownHook(hook: Hook): GridConfig = copy(shutdownHooks = shutdownHooks :+ hook)

	def withNodes(nodes: NodeConfig*): GridConfig = copy(nodes = nodes)
	def addNodes(addNodes: NodeConfig*): GridConfig = copy(nodes = nodes ++ addNodes)

	//  methods that allow us to narrow the node selection in our config
	def selectNodes(filter: NodeConfig => Boolean): GridConfig = copy(nodes = nodes.filter(filter))
	def selectRandomNode: GridConfig = copy(nodes = Seq(nodes(Random.nextInt(nodes.size))))
	def selectUserHashedNode: GridConfig = copy(nodes = Seq(nodes(Math.abs(System.getProperty("user.name").hashCode) % nodes.size)))
}


object GridConfig {

	def apply(name: String, nodes: NodeConfig*): GridConfig = GridConfig(name, nodes.toSeq)

	def localFork(id: String = "fork"): GridConfig = {
		apply(id, LocalNodeConfig(id)).withPropertyOverrides(System.getProperties)
	}

}
