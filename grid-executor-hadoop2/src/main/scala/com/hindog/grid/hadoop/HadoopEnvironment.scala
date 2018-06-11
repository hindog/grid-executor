package com.hindog.grid.hadoop

import com.hindog.grid.ClasspathUtils
import org.apache.commons.io._
import org.apache.commons.io.filefilter.SuffixFileFilter
import org.apache.hadoop.conf.Configuration

import java.io._
import java.net.{URL, URLClassLoader}
import java.util.concurrent.TimeUnit

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
object HadoopEnvironment {

  private val defaultFiles = Array("core-site.xml", "hdfs-site.xml", "yarn-site.xml", "mapred-site.xml")

  /**
    * Load Hadoop configuration using all files ending with ".xml" in `HADOOP_CONF_DIR`
    */
  def loadConfiguration(loadDefaults: Boolean): Configuration = loadConfiguration(loadDefaults, Array.empty)

  /**
    * Load Hadoop configuration using one of two methods:
    *
    * 1) If HADOOP_CONF_DIR is set, load all (or specified) resource files from that folder.
    *
    * -- otherwise --
    * 
    * 2) Use Hadoop classpath obtained from `hadoop classpath` command.  By default, we'll
    * load `core-site.xml`, `hdfs-site.xml`, `mapred-site.xml` and `yarn-site.xml` files we find on the classpath.
    */
  def loadConfiguration(loadDefaults: Boolean = true, files: Array[String] = Array.empty): Configuration = {
    import scala.collection.JavaConverters._
    Option(System.getenv("HADOOP_CONF_DIR")).map(dir => new File(dir)) match {
      case Some(confDir) => {
        val conf = new Configuration(loadDefaults)
        if (files.isEmpty) {
          FileUtils.listFiles(confDir, Array(".xml"), false).asScala.foreach(f => conf.addResource(f.toURI.toURL))
        } else {
          files.foreach(f => conf.addResource(new File(confDir, f).toURI.toURL))
        }
        conf
      }
      case None => {
        val confFiles = if (files.isEmpty) defaultFiles else files
        val hadoopClassloader = new URLClassLoader(classpath)
        val conf = new Configuration(loadDefaults)
        conf.setClassLoader(hadoopClassloader)
        confFiles.foreach(conf.addResource)
        conf
      }
    }
  }

  /**
    * Fairly portable method for getting the Hadoop classpath in the current environment.
    * It assumes that the 'hadoop' command is available and calls 'hadoop classpath', parses
    * the result, and returns a resolved list of URL's (including wildcard/glob-style patterns,
    * see `com.hindog.grid.ClasspathUtils` for more info).
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
