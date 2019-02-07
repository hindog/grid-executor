package com.hindog.grid.repo

import com.hindog.grid.Logging

import java.util.Properties

class LazyRepository(repoClass: Class[_ <: Repository], properties: Properties) extends Repository with Logging {
  def this(repoClass: Class[_ <: Repository]) = this(repoClass, new Properties())
  
  lazy val delegate: Repository = try {
    repoClass.getConstructor(classOf[Properties]).newInstance(properties)
  } finally logger.info(s"Instantiated repository of type ${repoClass.getName}")

  override def contains(resource: Resource): Boolean = delegate.contains(resource)
  override def get(filename: String, contentHash: String): Resource = delegate.get(filename, contentHash)
  override def put(res: Resource): Resource = delegate.put(res)
}

