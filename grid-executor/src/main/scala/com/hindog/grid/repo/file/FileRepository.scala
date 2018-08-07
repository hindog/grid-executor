package com.hindog.grid.repo.file
import better.files._
import com.hindog.grid._
import com.hindog.grid.repo.{Repository, Resource}
import org.apache.commons.codec.binary.Hex
import org.apache.commons.codec.digest.MessageDigestAlgorithms

import java.nio.file.{Path, Paths}
import java.security.{DigestOutputStream, MessageDigest}
import java.util.Properties

class FileRepository(properties: Properties) extends Repository {
  private val basePath: File = File(properties.getProperty("base-path", "~/.jar-cache"))
          .ifThen(_.toString.startsWith("~" + java.io.File.pathSeparator))(p => System.getProperty("user.home") / p.toString.substring(1))

  require(!basePath.isDirectory, s"$basePath must be a directory!")

  def path(filename: String, contentHash: String): File = basePath / contentHash / filename
  def path(resource: Resource): File = path(resource.contentHash, resource.filename)

  override def contains(resource: Resource): Boolean = {
    val file = basePath / resource.contentHash / resource.filename
    file.exists && file.size == resource.contentLength
  }

  override def get(filename: String, contentHash: String): Resource = new FileResource(filename, contentHash, path(filename, contentHash))

  override def put(resource: Resource): Resource = {
    if (contains(resource)) get(resource)
    else {
      val outFile = path(resource.filename, resource.contentHash)
      val sha1 = MessageDigest.getInstance(MessageDigestAlgorithms.SHA_1)
      
      val ret = for {
        in <- resource.inputStream.buffered.autoClosed
        out <- outFile.newOutputStream.buffered.autoClosed
        digest = new DigestOutputStream(out, sha1)
      } yield {
        in.pipeTo(out)
        new FileResource(resource.filename, Hex.encodeHexString(digest.getMessageDigest.digest()), outFile)
      }

      ret.get()
    }
  }

}

