package com.hindog.grid.repo

import java.net.{URI, URLClassLoader}

import com.github.igorsuhorukov.smreed.dropship.{MavenClassLoader, MavenDependency}
import com.hindog.grid.PostDelegatingURLClassLoader

import scala.collection._

/**
  * Created by atom-aaron on 6/3/17
  */
class ClassLoadingRepository(className: String, classloader: () => URLClassLoader) extends Repository {
  //private lazy val cl = MavenClassLoader.forMavenCoordinates(dependencies, getClass.getClassLoader)
  private lazy val repo = {
    val cl = new PostDelegatingURLClassLoader(classloader(), getClass.getClassLoader)
    cl.getURLs.foreach(println)
    cl.loadClass(className).newInstance().asInstanceOf[Repository]
  }

  override def uri: URI = repo.uri
  override def contains(resource: Resource): Boolean = repo.contains(resource)
  override def upload(resource: Resource): Resource = repo.upload(resource)
  override def resolve(resource: Resource): Resource = repo.resolve(resource)
  override def close(): Unit = repo.close()
}
