package com.hindog.grid.repo

import java.net.{URI, URLClassLoader}

import com.github.igorsuhorukov.smreed.dropship.MavenDependency
import com.hindog.grid.ClasspathUtils


/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
trait Repository extends Serializable {
  def uri: URI
  def contains(resource: Resource): Boolean
  def upload(resource: Resource): Resource
  def resolve(resource: Resource): Resource
  def close()
}

object Repository {

  val hdfs = new ClassLoadingRepository("com.hindog.grid.hadoop.HDFSRepository", () => new URLClassLoader(ClasspathUtils.executeCommandClasspath("/bin/bash", "hadoop", "classpath")))

//  val s3 = new ClassLoadingRepository("com.hindog.grid.hadoop.HDFSRepository", Array(
//    new MavenDependency("org.apache.hadoop:hadoop-client:2.7.3")
//  ))
}

