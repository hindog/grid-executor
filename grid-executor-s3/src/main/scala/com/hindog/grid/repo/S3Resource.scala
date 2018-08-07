package com.hindog.grid.repo

import com.amazonaws.services.s3.AmazonS3

import java.io.InputStream
import java.net.URI

class S3Resource(val filename: String, val contentHash: String, val contentLength: Long, protocol: String, bucketName: String, objectName: String, s3Client: AmazonS3) extends Resource {
  override def inputStream: InputStream = s3Client.getObject(bucketName, objectName).getObjectContent
  val uri: URI = new URI(s"$protocol://$bucketName/$objectName")
  override def exists: Boolean = s3Client.getObjectMetadata(bucketName, objectName) != null
}
