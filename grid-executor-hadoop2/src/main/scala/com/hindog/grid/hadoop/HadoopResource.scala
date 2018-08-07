package com.hindog.grid.hadoop

import com.hindog.grid.repo.Resource
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.InputStream
import java.net.URI

class HadoopResource(val filename: String, val contentHash: String, val contentLength: Long, path: Path, fs: FileSystem) extends Resource {
  override def inputStream: InputStream = fs.open(path)
  override def uri: URI =  fs.getUri.resolve(path.toString)
  override def exists: Boolean = fs.exists(path)
}

