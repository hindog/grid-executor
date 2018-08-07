package com.hindog.grid.repo.http

import com.hindog.grid.repo.Resource

import java.io.InputStream
import java.net._

class HttpResource(val filename: String, val contentHash: String, val contentLength: Long, val uri: URI) extends Resource {
  override def inputStream: InputStream = uri.toURL.openStream()
  
  override def exists: Boolean = {
    val conn = uri.toURL.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("GET")
    conn.connect()
    conn.getResponseCode == 200
  }
}
