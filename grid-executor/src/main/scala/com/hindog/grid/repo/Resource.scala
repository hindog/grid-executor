package com.hindog.grid.repo

import better.files._
import com.hindog.grid.{Logging, _}
import com.hindog.grid.repo.file.FileResource
import org.apache.commons.codec.binary.Hex
import org.apache.commons.codec.digest.{DigestUtils, MessageDigestAlgorithms}
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream
import org.apache.commons.compress.utils.IOUtils

import java.io.{FileInputStream, InputStream, File => JFile}
import java.net.{URI, URL}
import java.nio.file._
import java.security.{DigestOutputStream, MessageDigest}

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
trait Resource {
  def protocol: String = Option(uri.getScheme).map(_.toLowerCase).getOrElse("file")
  def filename: String
  def contentHash: String
  def contentLength: Long
  def inputStream: InputStream
  def uri: URI
  def exists: Boolean
  override def toString: String = uri.toString
}

object Resource extends Logging {

  private val bufferSize = 1024 * 1024 * 5 // 5mb buffer

  def url(url: URL): Resource = uri(url.toURI)
  
  def uri(uri: URI): Resource = {
    val f = File(uri)
    if ("file" == uri.getScheme.toLowerCase) file(f)
    else inputStream(f.name, f.newInputStream)
  }

  def file(f: Path): Resource = file(f.toFile)
  def file(f: JFile): Resource = file(f.getName, f)
  def file(f: File): Resource = file(f.name, f)
  def file(filename: String, f: JFile): Resource = file(filename, f.toPath)
  def file(filename: String, path: Path): Resource = file(filename, File(path))
  def file(filename: String, file: File): Resource = {
    if (file.isDirectory) Resource.dir(filename, file)
    else{
      file.inputStream() { is => new FileResource(filename, DigestUtils.sha1Hex(is), file.toJava) }
    }
  }

  def dir(d: Path): Resource = dir(File(d))
  def dir(d: JFile): Resource = dir(File(d.toPath))
  def dir(d: File): Resource = dir(d.name, d)
  def dir(filename: String, d: JFile): Resource = dir(filename, File(d.toPath))
  def dir(filename: String, d: Path): Resource = dir(filename, File(d))
  def dir(filename: String, d: File): Resource = {
    if (!d.isDirectory) throw new IllegalArgumentException(s"$d is not a directory!")
    val tmp = File.newTemporaryFile(filename + "-", ".jar")

    for {
      out <- new ZipArchiveOutputStream(tmp.newOutputStream).autoClosed
    } {
      d.listRecursively.foreach { file =>
        val relative = d.relativize(file).toString
        val entry = out.createArchiveEntry(file.toJava, relative)
        out.putArchiveEntry(entry)
        if (!file.isDirectory) {
          val is = (d / file.toString).newFileInputStream
          IOUtils.copy(is, out)
          is.close()
        }
        out.closeArchiveEntry()
      }
    }
    file(filename.ifThen(!_.endsWith(".jar"))(_ + ".jar"), tmp)
  }

  def inputStream(filename: String, is: InputStream): Resource = {
    val tmp = File.newTemporaryFile(filename + "-", ".jar")
    val sha1 = MessageDigest.getInstance(MessageDigestAlgorithms.SHA_1)

    val ret = for {
      in <- is.buffered(bufferSize).autoClosed
      out <- tmp.newOutputStream.buffered(bufferSize).autoClosed
      digest = new DigestOutputStream(out, sha1)
    } yield {
      in.pipeTo(out)
      new FileResource(filename, Hex.encodeHexString(digest.getMessageDigest.digest()), tmp.toJava)
    }

    ret.get()
  }
}


