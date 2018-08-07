package com.hindog.grid.repo.http

import com.hindog.grid.repo._

import java.util.Properties
import scala.collection.JavaConverters._

import java.net.URI

class DelegatingHttpRepository(props: Properties) extends Repository {

  private val hostname = props.get("hostname")
  private val port = Option(props.get("port")).map(_.toString.toInt).getOrElse(80)
  private val path = Option(props.get("path")).map(_.toString).getOrElse("/")

  private lazy val delegateProps = {
    val out = new Properties()
    out.putAll(props.asScala.filter(_._1.startsWith("delegate.")).map{ case (key, value) => key.stripPrefix("delegate.") -> value }.toMap.asJava)
    out
  }

  private lazy val delegate: Repository = Repository(delegateProps)

  protected def wrap(resource: Resource): Resource = {
    new HttpResource(resource.filename, resource.contentHash, resource.contentLength, new URI(s"http://$hostname:$port/${path.stripPrefix("/")}/${resource.contentHash}/${resource.contentHash}-${resource.filename}"))
  }

  override def contains(resource: Resource): Boolean = delegate.contains(resource)
  override def get(filename: String, contentHash: String): Resource = wrap(delegate.get(filename, contentHash))
  override def put(res: Resource): Resource = wrap(delegate.put(res))
}

