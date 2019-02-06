package com.hindog.grid
package repo

import com.amazonaws.auth._
import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model.ObjectMetadata

import scala.collection._

import java.util.Properties

/*
 *    __   _         __
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, /
 *                       /___/
 */
case class S3Repository(properties: Properties) extends Repository with Logging with RepositoryOps {

  @transient protected lazy val credentials = new DefaultAWSCredentialsProviderChain()
  @transient protected lazy val s3client: AmazonS3 = new AmazonS3Client(credentials.getCredentials)

  private val protocol = prop[String]("protocol", "s3a")
  private val bucketName = prop[String]("bucket", throw new RepositoryException("missing required property 'bucket'!"))
  private val prefix = prop[String]("prefix", "/").stripPrefix("/")

  @transient private lazy val cachedListing = {
    val listing = S3KeyIterator(s3client, bucketName, prefix)
    val resources = listing.flatMap { s =>
      val suffix = s.getKey.stripPrefix(prefix).stripPrefix("/")
      suffix match {
        case extractPath(contentHash, filename) => Some(new S3Resource(filename, contentHash, s.getSize, protocol, bucketName, s.getKey, s3client))
        case _ => None
      }
    }

    val out = mutable.HashMap[String, Resource](resources.map(r => r.contentHash -> r).toSeq: _*)
    logger.info(s"Cached ${out.size} object summaries into S3 repository")
    out
  }

  protected def objectName(filename: String, contentHash: String): String = s"$prefix/$contentHash/$contentHash-$filename"
  //protected def key(res: Resource): String = s"$prefix/${res.contentHash}/${res.contentHash}-${res.filename}"

  override def contains(resource: Resource): Boolean = cachedListing.contains(resource.contentHash) || s3client.getObjectMetadata(bucketName, objectName(resource.filename, resource.contentHash)) == null

  override def get(filename: String, contentHash: String): Resource = {
    cachedListing.getOrElse(contentHash, {
      val key = objectName(filename, contentHash)
      new S3Resource(filename, contentHash, s3client.getObjectMetadata(bucketName, key).getContentLength, protocol, bucketName, key, s3client)
    })
  }

  override def put(res: Resource): Resource = {
    // check if this resource exists in our cached listing, if not, we'll create it and update our cache
    cachedListing.get(res.contentHash) match {
      case Some(r) => r
      case None => {
        val meta = new ObjectMetadata()
        meta.setContentLength(res.contentLength)
        val key = objectName(res.filename, res.contentHash)
        s3client.putObject(bucketName, key, res.inputStream, meta)
        val s3Resource = new S3Resource(res.filename, res.contentHash, res.contentLength, protocol, bucketName, key, s3client)
        cachedListing.put(res.contentHash, s3Resource)
        logger.info(s"Uploaded $res (${res.contentLength / 1024} Kb) to s3://$bucketName/$key}")
        s3Resource
      }
    }
  }

}
