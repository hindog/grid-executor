package com.hindog.grid

import java.util
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy
import java.util.concurrent._
import com.hindog.grid.GridConfigurable.Keys
import com.hindog.grid.GridExecutor.Node
import io.github.classgraph.ClassGraph
import org.gridkit.nanocloud.{CloudFactory, VX}
import org.gridkit.nanocloud.telecontrol.HostControlConsole
import org.gridkit.vicluster._
import org.gridkit.vicluster.telecontrol.StreamCopyThread
import org.gridkit.vicluster.ViEngine.Interceptor

import scala.collection._
import scala.concurrent.{ExecutionContext, Future => SFuture}
import scala.util.Try
import scala.util.control.NonFatal

/*
 *    __   _         __
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, /
 *                       /___/
 *
 * Implementation of an ExecutorService that parallelizes work across multiple JVMs (either local and/or remote).
 *
 * It uses a library called NanoCloud (https://github.com/gridkit/nanocloud) which is fairly straight-forward
 * to use but it's somewhat obscure at the same time.  This class serves as an abstraction that exposes an
 * ExecutorService that can execute tasks across multiple JVMs.
 *
 * The 'GridConfig' is used to define a configuration the JVMs to use.
 *
 * You can also create an ExecutionContext around this and execute scala Future's remotely in a transparent
 * manner.  NOTE: Future callbacks (onComplete, andThen, map, etc) will be executed *locally* using the `global`
 * ExecutionContext.  If you wish to chain tasks so that the output of one grid task serves as the input of another
 * grid task (similar to the functional 'andThen`), you can use Future's `flatMap`.
 *
 * We could also extend this to support other cluster managers (ie: YARN).
 */
class GridExecutor protected (gridConfig: GridConfig) extends AbstractExecutorService with Logging {

	import scala.collection.JavaConverters._
	import GridExecutor._


	private val cloud = CloudFactory.createCloud()
	cloud.node("**").x(VX.TYPE).setIsolate()
	protected val threadFactory = Executors.defaultThreadFactory()
	protected val threadPool = Executors.newFixedThreadPool(Math.max(gridConfig.nodes.size, 1), new ThreadFactory {
		override def newThread(r: Runnable): Thread = {
			val t = threadFactory.newThread(r)
			t.setName("grid-executor [" + gridConfig.name + "] " + t.getName)
			t
		}
	}).asInstanceOf[ThreadPoolExecutor]
	threadPool.setRejectedExecutionHandler(new CallerRunsPolicy())

	private val viNodes = {
		val nodes = gridConfig.nodes.flatMap(node => {
			// create node (but not yet initialized)
			val viNode = node.create(cloud)

			// used to source properties for this cloud instance
			viNode.setProp(Keys.gridIdKey, gridConfig.name)
			
			// configure node (apply grid-level config and then node-level config)
			val instance = (gridConfig.config andThen node.config)(viNode)
			val slots = (node.slots orElse gridConfig.slots).getOrElse(1)
			(0 until slots).map(i => Node(instance, i + 1))
		})

		val cl = Thread.currentThread().getContextClassLoader
		try {
			Thread.currentThread().setContextClassLoader(ClasspathUtils.urlClassloader)
			cloud.node("**").touch()
			runHooks("startup", _.startupHooks)
		} catch {
			case NonFatal(ex) => { quietlyShutdownNow(); throw ex	}
		} finally {
			Thread.currentThread().setContextClassLoader(cl)
		}

		nodes
	}

	// tracks which nodes/slots are available for execution
	private val nodeDeque = new LinkedBlockingDeque[Node](viNodes.asJavaCollection)

	protected[grid] def withNode[T](work: Node => T): T = {
		val node = nodeDeque.take()
		try {
			work(node)
		} finally {
			nodeDeque.put(node)
		}
	}

	override def execute(runnable: Runnable): Unit = {
		runnable match {
			// detect if this is a scala future, if so we adapt a (somewhat hacky) special case to handle it
			case sf if sf.getClass.getName == "scala.concurrent.impl.Future$PromiseCompletingRunnable" => threadPool.execute(ScalaFutureGridTask(runnable)(this))
			case gt: GridTask[_] => threadPool.execute(gt(this))
			// execute all other tasks (callbacks, etc) locally using the global ExecutionContext
			case _ => ExecutionContext.global.execute(runnable)
		}
	}


	override def newTaskFor[T](runnable: Runnable, value: T): RunnableFuture[T] = {
		runnable match {
			case g: GridTask[T @unchecked] => g(this)
			case other => GridTask.runnable[T](other, value)(this)
		}
	}

	override def newTaskFor[T](callable: Callable[T]): RunnableFuture[T] = GridTask.callable[T](callable)(this)

	override def isTerminated: Boolean = threadPool.isTerminated

	override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = threadPool.awaitTermination(timeout, unit)

	override def shutdown(): Unit = {
		shutdownUsing {
			if (threadPool.getActiveCount > 0)
				logger.info(s"Shutting down GridExecutor '${gridConfig.name}' after ${threadPool.getActiveCount} active tasks complete...")
			else
				logger.info(s"Shutting down GridExecutor '${gridConfig.name}'")

			threadPool.shutdown()
		}
	}

	def quietlyShutdownNow(): Unit = {
		shutdownUsing {
			if (threadPool != null)	{
				threadPool.shutdown()
				threadPool.awaitTermination(1, TimeUnit.SECONDS)
				threadPool.shutdownNow()
			}
		}
	}

	override def shutdownNow(): util.List[Runnable] = {
		shutdownUsing {
			if (threadPool.getActiveCount > 0)
				logger.info(s"Shutting down GridExecutor '${gridConfig.name}' IMMEDIATELY! (${threadPool.getActiveCount} active tasks still running!)")
			else
				logger.info(s"Shutting down GridExecutor '${gridConfig.name}'")
			threadPool.shutdownNow()
		}
	}

	override def isShutdown: Boolean = threadPool.isShutdown

	protected[grid] def shutdownUsing[T](thunk: => T): T = {
		try {
			runHooks("shutdown", _.shutdownHooks)
		} catch {
			case NonFatal(ex) => logger.warn("Exception occurred while shutting down cloud", ex)
		}

		try { thunk } finally { Try(cloud.shutdown()).recover{ case ex => logger.warn("Exception while shutting down grid", ex)} }
	}

	protected[grid] def runHooks(name: String, f: GridConfigurable => Seq[Hook]) = {
		try {
			val hooks = f(gridConfig)
			if (hooks.nonEmpty) {
				logger.info(s"Running $name hooks [${hooks.map(_.name).mkString(", ")}]")

				// run cloud-specific hooks
				f(gridConfig).foreach (hook =>
					//info("run")
					cloud.node("**").massSubmit(hook).asScala.foreach(_.get())
				)

				// run node-specific hooks
				val hostInits = gridConfig.nodes.groupBy(n => n.name).map{ case (host, configs) => host -> configs.flatMap(f) }.mapValues(hooks =>
					new Runnable with Serializable {
						override def run(): Unit = {
							hooks.foreach(_.run())
						}
					}
				)

				val latch = new CountDownLatch(gridConfig.nodes.size)
				hostInits.foreach{ case (host, init) => {
					threadPool.submit(new Callable[Unit] {
						override def call(): Unit = {
							cloud.node(host).exec(init)
							latch.countDown()
						}
					})
				}}

				latch.await(30L, TimeUnit.SECONDS)
			}
		} catch {
			case NonFatal(ex) => logger.warn(s"Exception while running $name hooks", ex)
		}
	}

	try {
		Runtime.getRuntime.addShutdownHook(new Thread() {
			override def run(): Unit = {
				if (!isShutdown) {
					shutdown()
				}
			}
		})
	} catch {
		case ex: Exception => logger.warn("Unable to register shutdown hook for grid! JVM already shutting down?")
	}

}

object GridExecutor {

	case class Node(node: ViNode, slot: Int) extends Serializable

	// Creates a new GridExecutor whose life-cycle will be externally managed (ie: caller is responsible for calling 'shutdown')
	def apply(config: GridConfig): GridExecutor = new GridExecutor(config)

	/*
		Single-use grid executor that wraps grid initialization and user code into a single Scala Future[T].
		By default, grid initialization will occur using the 'scala.concurrent.ExecutionContext.global' ExecutionContext

		If you plan on submitting multiple futures, consider creating an implicit value of
		scala.concurrent.ExecutionContext that wraps an instance of GridExecutor.
	 */
	def future[T](config: GridConfig)(thunk: => T)(implicit ec1: ExecutionContext = scala.concurrent.ExecutionContext.global): SFuture[T] = {

		// initialize the grid as part of the future's execution
		SFuture {
			scala.concurrent.blocking {
				var ge: GridExecutor = null
				try {
					ge = new GridExecutor(config)
					ge
				} catch {
					case NonFatal(ex) =>
						if (ge != null) { ge.quietlyShutdownNow() }
						throw ex
				}
			}
		}.flatMap(executor => {
			implicit val ec = scala.concurrent.ExecutionContext.fromExecutorService(executor)
			// execute 'thunk' and then shutdown executor after completion
			SFuture { thunk }.transform(
				ret => { executor.quietlyShutdownNow(); ret },
				ex => { executor.quietlyShutdownNow(); ex }
			)
		})
	}

	// Creates a new GridExecutor whose lifetime is scoped to 'thunk'.  Useful for multiple ad-hoc invocations
	def withInstance[T](config: GridConfig)(thunk: GridExecutor => T): T = {
		val executor = new GridExecutor(config)
		try {
			thunk(executor)
		} finally {
			executor.shutdown()
		}
	}
}
