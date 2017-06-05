package com.hindog.grid

import java.io.{File, FileFilter, StringWriter}
import java.net.{JarURLConnection, URL, URLDecoder}
import java.util.concurrent.TimeUnit

import org.apache.commons.io.IOUtils

import scala.collection.JavaConverters._
import scala.collection._

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
object ClasspathUtils {

  def listCurrentClasspath: Array[URL] = try {
    org.gridkit.vicluster.telecontrol.ClasspathUtils.listCurrentClasspath().asScala.toArray
  } catch {
    case _: Exception => Array.empty
  }

  /**
    * Fairly portable method for getting the Hadoop classpath in the current environment.
    * It assumes that the 'hadoop' command is available and calls 'hadoop classpath', parses
    * the result, and returns a resolved list of URL's (including wildcard/glob-style patterns,
    * see `com.hindog.grid.ClasspathUtils` for more info).
    */
  def executeCommandClasspath(cmd: String*): Array[URL] = {
    val cpProcess = new ProcessBuilder(cmd: _*).start()
    val stringWriter = new StringWriter()

    cpProcess.waitFor(10, TimeUnit.SECONDS)
    IOUtils.copy(cpProcess.getInputStream, stringWriter)

    ClasspathUtils.explodeClasspath(stringWriter.toString)
  }


  /**
    * Given a raw classpath string, try to resolve a list of URL's including wild-card and MANIFEST.MF entries
    */
  def explodeClasspath(classpath: String = System.getProperty("java.class.path")): Array[URL] = {
    classpath.split(File.pathSeparator).flatMap(file => {
      if (file.endsWith(".jar")) {
        try {
          val url = new URL("jar:file:" + file + "!/META-INF/MANIFEST.MF")
          val jarConnection = url.openConnection().asInstanceOf[JarURLConnection]
          val manifest = new java.util.jar.Manifest(jarConnection.getInputStream)
          manifest.getMainAttributes.getValue("Class-Path").split("\\s+").map(f => URLDecoder.decode(f, "UTF-8")).toSeq
        } catch {
          case ex: Exception => Seq(file)
        }
      } else if (file.endsWith("*")) {
        new File(file.stripSuffix("*")).listFiles(new FileFilter {
          override def accept(pathname: File): Boolean = pathname.getName.endsWith(".jar") || pathname.getName.endsWith(".xml") || pathname.getName.endsWith(".properties") || pathname.getName.endsWith(".class")
        }).map(_.toString).toSeq
      } else {
        Seq(file)
      }
    }).map(s => new File(s).toURI.toURL)
  }
}
