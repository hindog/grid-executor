package com.hindog.grid.spark.shell

import java.io.{BufferedReader, PrintWriter}
import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation.{Around, Aspect}

import scala.tools.nsc.{interpreter, Settings}
import scala.tools.nsc.interpreter.{ILoop, JPrintWriter}

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */

class InterceptedILoop(in: Option[BufferedReader], out: PrintWriter) extends ILoop(in, out) {
  @scala.deprecated("Use `process` instead", "October 4, 2016")
  override def main(settings: Settings): Unit = process(settings)

  override def process(settings: Settings) = {
    val mainClass = Class.forName(Option(System.getProperty("shell.main.class")).getOrElse(throw new RuntimeException("'shell.main.class' not set.  Please add a -Dshell.main.class=<main class> to your VM options")))
    val instance = mainClass.getField("MODULE$").get(null).asInstanceOf[{ def main(args: Array[String]) }]
    instance.main(Array.empty)
    true
  }
}

@Aspect
class SparkShellAspect {

  @Around(value = "call(scala.tools.nsc.interpreter.ILoop.new(..)) && args(in, out)")
  def interceptMain(pjp: ProceedingJoinPoint, in: Option[BufferedReader], out: PrintWriter): AnyRef = {
    new InterceptedILoop(in, out)
  }

  @Around(value = "call(scala.tools.nsc.interpreter.ILoop.new())")
  def interceptMain(pjp: ProceedingJoinPoint): AnyRef = {
    new InterceptedILoop(None, new interpreter.JPrintWriter(Console.out, true))
  }
}

