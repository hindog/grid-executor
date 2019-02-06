package com.hindog.grid.repo.file

import com.hindog.grid.repo.Resource

import java.io._
import java.net.URI
import java.nio.file._

class FileResource(val filename: String, val contentHash: String, file: File) extends Resource {
  lazy val contentLength = file.length()
  lazy val uri: URI = file.toURI
  override def exists: Boolean = file.exists()
  override def inputStream: InputStream = new FileInputStream(file)
}
