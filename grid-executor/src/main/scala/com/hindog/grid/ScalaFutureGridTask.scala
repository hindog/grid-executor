package com.hindog.grid

import scala.concurrent.Promise

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 *
 * Scala's Future (specifically scala.concurrent.impl.Future$PromiseCompletingRunnable) is not serializable
 *
 * The use case for running a Future remotely via serialization was probably not considered, but in our case
 * it's valid and useful to allow us to compose Future { } blocks that can execute remotely.
 *
 * Here's what we do to execute the future remotely:
 *
 * 1) Only run if we are dealing with a 'scala.concurrent.impl.Future$PromiseCompletingRunnable'
 * 2) Read the 'body' field using reflection (the block of code to execute) as a Function0[Any]
 * 3) Read the 'promise' field using reflection as a Promise[Any]
 * 4) Wrap the 'body' in a new Callable[T] that executes remotely and will return the result
 * 5) Complete the 'promise' manually
 * 6) Wrap all of this into a new Runnable to be passed back to GridExecutor
 *
 */
protected[grid] object ScalaFutureGridTask  {

	private val scalaFutureClazz = Class.forName("scala.concurrent.impl.Future$PromiseCompletingRunnable")
	private val bodyField = scalaFutureClazz.getDeclaredField("body")
	private val promiseField = scalaFutureClazz.getDeclaredField("promise")
	promiseField.setAccessible(true)
	bodyField.setAccessible(true)

	def apply(runnable: Runnable): GridTask[Any] = {
		val body = bodyField.get(runnable).asInstanceOf[() => Any]
		val promise = promiseField.get(runnable).asInstanceOf[Promise[Any]]
		GridTask[Any](body(), promise.complete)
	}

}
