package com.hindog.grid.hadoop

import java.io.StringWriter
import java.net.{URL, URLClassLoader}
import java.util.concurrent.TimeUnit

import com.hindog.grid.ClasspathUtils
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
object HadoopEnvironment {

  /**
    * Load Hadoop configuration using detected Hadoop classpath obtained from [[classpath]].  By default, we'll
    * load `core-site.xml` and `hdfs-site.xml`
    */
  def loadConfiguration(files: Array[String] = Array("core-site.xml", "hdfs-site.xml")): Configuration = {
    val hadoopClassloader = new URLClassLoader(classpath)
    val conf = new Configuration(true)
    conf.setClassLoader(hadoopClassloader)
    files.foreach(conf.addResource)
    conf
  }

  /**
    * Fairly portable method for getting the Hadoop classpath in the current environment.
    * It assumes that the 'hadoop' command is available and calls 'hadoop classpath', parses
    * the result, and returns a resolved list of URL's (including wildcard/glob-style patterns,
    * see [[com.hindog.grid.ClasspathUtils.explodeClasspath()]] for ).
    */
  def classpath: Array[URL] = {
    val cpProcess = new ProcessBuilder("hadoop", "classpath").start()
    val stringWriter = new StringWriter()

    cpProcess.waitFor(10, TimeUnit.SECONDS)
    IOUtils.copy(cpProcess.getInputStream, stringWriter)

    ClasspathUtils.explodeClasspath(stringWriter.toString)
  }

  def withHadoopClassloader[T](thunk: => T): T = {
    val parent = Thread.currentThread().getContextClassLoader
    try {
      Thread.currentThread().setContextClassLoader(new URLClassLoader(classpath))
      thunk
    } finally {
      Thread.currentThread().setContextClassLoader(parent)
    }
  }
}
