package com.hindog.grid

import java.util.concurrent.{Callable, FutureTask, RunnableFuture}

import com.twitter.chill.Externalizer
import org.gridkit.vicluster.ViNode
import org.gridkit.zerormi.RmiGateway.CallableRunnableWrapper

import scala.util.{Failure, Success, Try}

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */

/*
	Low-level interface for defining grid tasks.

	Implementations can decide what work to execute, where to execute it and composes an (optional) result from the work.
 */
trait GridTask[T] {
	def apply(executor: GridExecutor): RunnableFuture[T]
}

/*
	Base implementation for a single-node task execution.
	Selects any available node and executes the `Callable[T]` on it.
 */
class NodeTask[T](work: GridExecutor.Node => Callable[T], onComplete: Try[T] => Unit = GridTask.emptyCallback) extends GridTask[T] {
	import GridTask._

	def apply(executor: GridExecutor): RunnableFuture[T] = {
		new FutureTask[T](new Callable[T] {
			override def call(): T = executor.withNode(node => {
				val remote = remoteCallable(work(node))
				node.node.exec(remote).get
			})
		}) {
			override def set(v: T): Unit = { super.set(v); onComplete(Success(v)) }
			override def setException(t: Throwable): Unit = { super.setException(t); onComplete(Failure(t)) }
		}
	}
}

object GridTask {

	private[grid] def emptyCallback[T]: Try[T] => Unit = _ => {}

	def apply[T](work: => T, onComplete: Try[T] => Unit = emptyCallback): GridTask[T] = callable(new Callable[T] {
		override def call(): T = work
	}, onComplete)

	def callable[T](callable: Callable[T], onComplete: Try[T] => Unit = emptyCallback): GridTask[T] = new NodeTask[T](_ => callable, onComplete)

	def runnable[T](runnable: Runnable, value: T): GridTask[T] = callable(new CallableRunnableWrapper[T](runnable, value))

	protected[grid] def remoteCallable[T](callable: Callable[T]): Callable[Externalizer[T]] = {
		val _callable = callable
		val closure = createClosure(_callable.call())

		val serializedClosure = new Externalizer[() => T]()
		serializedClosure.set(closure)

		new Callable[Externalizer[T]] with Serializable {
			override def call(): Externalizer[T] = {
				val result = new Externalizer[T]()
				result.set(serializedClosure.get.apply())
				result
			}
		}
	}

	protected[grid] def createClosure[T](work: => T): () => T = {
		val closure = () => work
		ClosureCleaner.clean(closure, checkSerializable = false)
		closure
	}

}
