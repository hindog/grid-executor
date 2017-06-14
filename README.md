## Grid Executor ##

This project allows for remote-execution of JVM code with the only remote dependency being password-less SSH.  

#### Features ####

* Zero-deployment remote JVM execution
* Implements `ExecutorService` to support submitting `Runnable` and/or `Callable[T]` to the grid nodes.
* Contains hooks for Scala `Future[T]` to allow for transparent grid execution by wrapping the `GridExecutor` in a Scala `ExecutionContext`.
* By default, the library will bind remote STDOUT/STDERR to local STDOUT/STDERR and optionally STDIN can be bound as well.
* Support for remote Spark/Hadoop execution from IDE for fast, iterative development and feedback (ie: `spark-submit` or `hadoop` on a hadoop gateway box, without manually uploading jars).
* Support for "IDE-pimped" `spark-shell` that gives you full power of the IDE's completion/import/copy-paste support while interacting with a shell running remotely on the cluster! (See [SparkShellExample.scala](https://github.com/hindog/grid-executor/blob/master/grid-executor-examples/src/main/scala/com/hindog/grid/examples/SparkShellExample.scala) for instructions) 
* Open-Source, Apache 2.0 License

#### Import ####

```scala
import com.hindog.grid._
```

#### Configuration ####

Configuration is provided via the `GridConfig` builder methods or a properties file (or both).  Properties are scoped by `grid.`.  

```

# Configures your remote username (if different from your local username)

grid.remote\:account.<local username>=<remote username>

# Adds JVM arg to ALL remote executions
grid.jvm\:xx\:permgen=-XX:MaxPermSize=768M
 
# Sets GridKit's "remote-runtime:jar-cache" property to determine where to store jars remotely (here we override the default [/tmp/nanocloud] to the user's [~/.jar-cache] on the remote box)
grid.remote-runtime\:jar-cache=.jar-cache
 
# Sets GridKit's "node:config-trace" property to dump ViEngine config on startup
grid.node\:config-trace=true
 
# Adds JVM args specific to remote executions on 'myGrid' nodes
grid.jvm\:xx\:mx.myGrid=-Xmx8g
grid.jvm\:exec-command.myGrid=/path/to/java
 
# User override to enable remote debug server that client can connect to
#grid.jvm\:xx\:debug.myGrid.ahiniker=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5004
 
# User override to enable remote debug client that will connect to our IDE on startup (replace the IP with your laptop's IP)
#grid.jvm\:xx\:debug.myGrid.ahiniker=-agentlib:jdwp=transport=dt_socket,server=n,address=10.170.1.45:5004,suspend=y
```

#### Examples ####

All of the examples below assume the following imports/base trait to provide some default grid definitions.  Also, it references a classpath resource of `grid.properties` that contains any configuration properties as outlined above.

NOTE: the grid definitions refer to `server1.example.com` and `server2.example.com`, these should be replaced with hostnames configured on your network.  Password-less SSH needs to be configured for each host. 

```scala
package com.hindog.grid.examples

import java.lang.management.ManagementFactory
import java.util.concurrent.Callable

import com.hindog.grid.GridConfigurable.Hook
import com.hindog.grid._
import scala.concurrent.duration._
import scala.concurrent._

trait GridExampleApp extends App {

	def message(msg: String = "Hello!") = s"$msg [thread: " + Thread.currentThread().getId + " within process: " + ManagementFactory.getRuntimeMXBean().getName() + "]"

	val configOneRemote: GridConfig = GridConfig(
		"myGrid",
		RemoteNodeConfig("server1.example.com", "server1") // host + alias
	).withPropertyOverrides(System.getProperties).withPropertyOverrides(properties)

	val configTwoRemote: GridConfig = GridConfig(
		"myGrid",
		RemoteNodeConfig("server1.example.com", "server1"), // host + alias
		RemoteNodeConfig("server2.example.com", "server2") // host + alias
	).withPropertyOverrides(System.getProperties).withPropertyOverrides(properties)
}

```

#### Scala ExecutionContext / Future Example ####

Demonstrates how we can use GridExecutor with Scala's `Future[T]` natively without any GridExecutor specific code.  Parallel collections are not supported.

```scala
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
```

#### Single-use Example ####

Demonstates how to initialize a grid whose life-cycle is scoped to a single task.

NOTE: the overhead in instantiating the cloud will be incurred on each invocation (as part of the future), but once the jars have sync'ed then subsequent invocations will have reduced overhead.

```scala
object GridExecutorSingleFutureExample extends GridExampleApp {

	import scala.collection.JavaConverters._
	import scala.concurrent.ExecutionContext.Implicits.global

	val fut = GridExecutor.future(configOneRemote) {
		println(message())
		System.getenv().asScala.toSeq.sortBy(_._1)
	}

	Await.result(fut, Duration.Inf).foreach(kv => println(kv._1 + "=" + kv._2))
}
```

#### Multi-use Example ####

Demonstates how to initialize a grid whose life-cycle is scoped to `thunk`.  Can be used to submit multiple tasks in an ad-hoc fashion.

```scala
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
```

#### Startup/Shutdown Hooks Example ####

Startup/Shutdown hooks allow arbirary code to be registered for execution as part of each node's startup or shutdown sequence.

```scala
object GridExecutorScopedWithInitializationExample extends Logging {
	var globalValue: String = "default value"

	def main(args: Array[String]) = {

		// Define an initialization process by using 'addStartupHook(new Hook("name") {...})'

		// below we will add a hook to set the 'globalValue' on the remote box on startup
		val baseConfig: GridConfig = GridConfig(
			"myGrid",
			RemoteNodeConfig("server1.example.com", "server1")
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
				info("running delay")
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
```

#### Local Fork Example ####

Demonstrates how to configure a local node that can be used for running code in a forked JVM.

```scala
object GridExecutorLocalForkExample extends App {
	import scala.concurrent.ExecutionContext.Implicits.global

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
```

### TODO / Gotchas ###

##### Spark / Hadoop Dependencies #####
For remote Spark/Hadoop execution, if your `App` class contains method signatures that reference classes from `provided` cluster jars, then the execution will fail unless those libraries are configured for `compile` scope.  Another work-around is to remove all traces of such classes in your `App` class method/field signatures and delegate to another class with your job's logic from within the body of the `run` method (method bodies aren't validated by the JVM on startup).  This will be addressed in an upcoming `2.0` release.

##### Spark Shell #####
##### Auth Errors #####
If you experience a `JSchAuthCancelException` or similar when running, it is most likely because your SSH key is not of the required minimum length (2048 bits).  Try generating a new key that is at least 2048 bits in length. 

##### TypeSafe Config Support #####
Upcoming `2.x` release will have an overhauled configuration process that allows for nested/inherited grid configs.  This will minimize the effort required for configuration while also providing good flexibility for per-grid, per-host, per-job, per-user configuration options, etc.

##### Diagram #####
***Coming Soon***