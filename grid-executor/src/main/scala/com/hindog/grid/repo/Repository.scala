package com.hindog.grid.repo

import com.hindog.grid.repo.file.FileRepository

import scala.util.control.NonFatal

import java.io._
import java.nio.file.Files
import java.util.Properties


/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
trait Repository extends Serializable {
  def contains(resource: Resource): Boolean
  def get(filename: String, contentHash: String): Resource
  def get(res: Resource): Resource = get(res.filename, res.contentHash)
  def put(res: Resource): Resource
  def close(): Unit = {}
  override def toString: String = getClass.getName
}

object Repository {

  def apply(props: Properties): Repository = {
    val className = Option(props.get("class-name")).getOrElse(throw new RepositoryException("Missing property 'class-name' for Repository config")).toString
    try {
      Thread.currentThread().getContextClassLoader.loadClass(className).getConstructor(classOf[Properties]).newInstance(props).asInstanceOf[Repository]
    } catch {
      case NonFatal(ex) => throw new RepositoryException(s"Failed to create Repository of type '$className'", ex)
    }
  }

  def localTemp(nested: Boolean = false, prefix: String = "jar-cache-"): Repository = {
    val tmpDir = Files.createTempDirectory(prefix)
    val props = new Properties()
    props.put("base-path", tmpDir.toString)
    props.put("nested", nested.toString)
    FileRepository(props)
  }
}