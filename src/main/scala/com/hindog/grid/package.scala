package com.hindog

import java.net.{InetAddress, NetworkInterface}
import scala.collection.JavaConverters._
/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
package object grid {
	/*
		Attempt to determine which IP is bound to the private subnet.
		Used to obtain a bind address that external boxes (dev, etc) can use to connect to us
	 */
	def subnetAddress(prefixes: Seq[String] = Seq("10.", "192.168.")): Option[InetAddress] = {
		val interfaces = NetworkInterface.getNetworkInterfaces.asScala.toSeq.flatMap(_.getInterfaceAddresses.asScala).map(_.getAddress)
		interfaces.find(ni => prefixes.exists(p => ni.getHostAddress.startsWith(p)))
	}
}
