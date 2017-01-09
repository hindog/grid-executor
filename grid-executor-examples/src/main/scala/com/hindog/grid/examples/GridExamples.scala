package com.hindog.grid.examples

import java.lang.management.ManagementFactory
import java.util.Properties
import java.util.concurrent.Callable

import com.hindog.grid._

import scala.concurrent._
import scala.concurrent.duration._

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */

trait GridExampleApp extends App with Logging {

	val properties = {
		val props = new Properties()
		props.load(getClass.getResourceAsStream("/grid.properties"))
		props
	}

	def message(msg: String = "Hello!") = s"$msg [thread: " + Thread.currentThread().getId + " within process: " + ManagementFactory.getRuntimeMXBean().getName() + "]"

	val configOneRemote: GridConfig = GridConfig(
		"gridExample",
		RemoteNodeConfig("jpc-gateway1.aws.prod.grv", "jpc-gateway1")
	).withPropertyOverrides(System.getProperties).withPropertyOverrides(properties)

	val configTwoRemote: GridConfig = GridConfig(
		"gridExample",
		RemoteNodeConfig("jpc-gateway1.aws.prod.grv", "jpc-gateway1"),
		RemoteNodeConfig("jpc-gateway1.aws.prod.grv", "jpc-gateway1")
	).withPropertyOverrides(System.getProperties).withPropertyOverrides(properties)
}

/*
 Scala ExecutionContext example

 Demonstrates how we can use GridExecutor with scala.concurrent types (like Future[T], parallel collections, etc)
 natively without any GridExecutor specific code
*/
object GridExecutorScalaFutureExample extends GridExampleApp {

	val remoteNodeConfig = configOneRemote.nodes.head

	val remoteNode1 = remoteNodeConfig.withName(remoteNodeConfig.name + "-1")
	val remoteNode2 = remoteNodeConfig.withName(remoteNodeConfig.name + "-2")
	val localNode1 =  LocalNodeConfig("local-1")
	val localNode2 =  LocalNodeConfig("local-2")

	// Set our config to use 2 remote and 2 local execution slots (4 total)
	val config2 = configOneRemote.withNodes(remoteNode1, remoteNode2, localNode1, localNode2)

	// create an implicit ExecutionContext to execute against
	implicit val ec = ExecutionContext.fromExecutorService(GridExecutor(config2))

	// No references to GridExecutor are present in the following code
	// This will use scala's Future to run tasks in parallel using remote JVMs
	// We throw a Thread.sleep to simulate real work, total time should reflect parallel execution completed
	// the work in less time than sequential execution
	val start = System.currentTimeMillis()

	val futures = (0 to 20).map(i => Future {
		println(message(s"executing task $i"))
		Thread.sleep(1000)
		s"result $i"
	})

	val results = Await.result(Future.sequence(futures), Duration.Inf)
	println(s"results = $results")
	println("total time: " + (System.currentTimeMillis() - start) + "ms")
	ec.shutdown()
}


/*
 Single-use grid executor
 Note that the overhead in instantiating the cloud will be incurred on each invocation (as part of the future),
 but once the jars have sync'ed then subsequent invocations will have reduced overhead
*/
object GridExecutorSingleFutureExample extends GridExampleApp {

	import scala.collection.JavaConverters._
	import scala.concurrent.ExecutionContext.Implicits.global

	val fut = GridExecutor.future(configOneRemote) {
		println(message())
		System.getenv().asScala.toSeq.sortBy(_._1)
	}

	Await.result(fut, Duration.Inf).foreach(kv => println(kv._1 + "=" + kv._2))
}

/*
	Multi-use grid executor whose life-cycle is scoped to 'thunk'.
	Can be used to submit multiple tasks in an ad-hoc fashion
*/
object GridExecutorScopedMultiUseExample extends GridExampleApp {

	// Submit 2 tasks and print their results
	GridExecutor.withInstance(configTwoRemote) { executor =>
		val fut1 = executor.submit(new Callable[String] {
			override def call(): String = {
				println("started task A")
				Thread.sleep(5000)
				message("result A")
			}
		})

		val fut2 = executor.submit(new Callable[String] {
			override def call(): String = {
				println("started task B")
				Thread.sleep(5000)
				message("result B")
			}
		})

		println("Future 1 result: " + fut1.get())
		println("Future 2 result: " + fut2.get())
	}

}

/*
	Demonstrates how to use startup/shutdown hooks
 */
object GridExecutorScopedWithInitializationExample extends Logging {
	var globalValue: String = "default value"

	def main(args: Array[String]) = {

		// Define an initialization process by using 'addStartupHook(new Hook("name") {...})'

		// below we will add a hook to set the 'globalValue' on the remote box on startup
		val baseConfig: GridConfig = GridConfig(
			"gridExample",
			RemoteNodeConfig("jpc-gateway1.aws.prod.grv", "jpc-gateway1")
		).withPropertyOverrides(System.getProperties)

		val config = baseConfig.addStartupHook(new Hook("my init hook") {
			override def run(): Unit = {
				// modify our global variable
				println("running initialization...")
				println(GridExecutorScopedWithInitializationExample.globalValue)
				GridExecutorScopedWithInitializationExample.globalValue = "initialized value"
			}
		}).addShutdownHook(new Hook("my shutdown hook") {
			override def run(): Unit = {
				logger.info("running delay")
				Thread.sleep(1000)
			}
		})

		val fut = GridExecutor.future(config) {
			// should return the value set via our initialization hook
			GridExecutorScopedWithInitializationExample.globalValue
		}

		// should reflect the init'ed value
		println("remote globalValue: " + Await.result(fut, Duration.Inf))
		// local value should be unchanged
		println("local globalValue: " + globalValue)
	}
}

/*
	Demonstrates how to configure a local node that can be used for running code in a forked JVM
 */
object GridExecutorLocalForkExample extends App {
	import scala.concurrent.ExecutionContext.Implicits.global
	/*
		localFork(id) is implemented as:

		GridConfig(
			id,
			LocalNodeConfig(id)
		).withPropertyOverrides(Settings.getProperties).withPropertyOverrides(System.getProperties)

	*/
	println("host jvm: " + ManagementFactory.getRuntimeMXBean.getName)
	val config1: GridConfig = GridConfig.localFork("fork 1").withMaxHeap("20m").withMinHeap("20m")
	val config2: GridConfig = GridConfig.localFork("fork 2").withMaxHeap("40m").withMinHeap("40m")

	val fut1: Future[Unit] = GridExecutor.future(config1) {
		println("forked jvm 1: " + ManagementFactory.getRuntimeMXBean.getName)
		println("total memory 1: " + Runtime.getRuntime.totalMemory())
		Thread.sleep(5000)
	}

	val fut2: Future[Unit] = GridExecutor.future(config2) {
		println("forked jvm 2: " + ManagementFactory.getRuntimeMXBean.getName)
		println("total memory 2: " + Runtime.getRuntime.totalMemory())
		Thread.sleep(5000)
	}

	Await.result(Future.sequence(Seq(fut1, fut2)), 10 seconds)

}