package com.hindog.grid

import io.github.classgraph.ClassGraph
import org.gridkit.vicluster.{ViEngine, ViExecutor}
import org.gridkit.vicluster.ViEngine.Interceptor

import java.io.{File, FileFilter}
import java.net.{JarURLConnection, URL, URLClassLoader, URLDecoder}
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
    val classpathScanner = new ClassGraph().addClassLoader(Thread.currentThread().getContextClassLoader).scan(4)
    classpathScanner.getAllResources.asScala.map(_.getClasspathElementURL).distinct.toArray
  } catch {
    case ex: Exception =>
      ex.printStackTrace()
      Array.empty
  }

  def urlClassloader: URLClassLoader = {
    new URLClassLoader(listCurrentClasspath)
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


