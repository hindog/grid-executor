package com.hindog.grid.repo.file

import better.files._
import com.hindog.grid.repo.Resource

import java.io.InputStream
import java.net.URI

class FileResource(val filename: String, val contentHash: String, file: File) extends Resource {
  lazy val contentLength = file.size
  lazy val uri: URI = file.uri
  override def exists: Boolean = file.exists()
  override def inputStream: InputStream = file.newInputStream
}
