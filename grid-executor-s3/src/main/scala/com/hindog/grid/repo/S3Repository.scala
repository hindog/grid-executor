package com.hindog.grid
package repo

import java.net.URI

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, EnvironmentVariableCredentialsProvider}
import com.amazonaws.regions.{AwsEnvVarOverrideRegionProvider, AwsRegionProvider}
import com.amazonaws.services.s3.AmazonS3ClientBuilder

import scala.reflect.ClassTag

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
case class S3Repository(credentialsProviderClass: SerializableCredentialsProvider, regionProviderClass: SerializableRegionProvider, bucketName: String, pathPrefix: String) extends Repository with Logging {

  @transient lazy val s3 = {
    AmazonS3ClientBuilder.standard().withCredentials(credentialsProviderClass).withRegion(regionProviderClass.getRegion).build()
  }

  @transient private lazy val prefix = pathPrefix.stripPrefix("/")

  override def uri: URI = new URI(s3.getBucketLocation(bucketName) + "/" + prefix)

  override def contains(resource: Resource): Boolean = s3.doesObjectExist(bucketName, (uri / resource.path).toString)

  override def upload(resource: Resource): Resource = {
    val targetResource = resolve(resource)
    logger.debug(s"Uploading ${resource.uri} to $targetResource")
    s3.putObject(bucketName, targetResource.uri.getPath, resource.fetch)
    targetResource
  }

  override def resolve(resource: Resource): Resource = Resource(uri, resource.path.replaceAll("/", "-"))

  override def close: Unit = {}
}

object S3Repository {

  def apply(credentialsProvider: SerializableCredentialsProvider, region: String, bucketName: String, pathPrefix: String): S3Repository = {
    new S3Repository(credentialsProvider, new SerializableRegionProvider {
      override def getRegion: String = region
    }, bucketName, pathPrefix)
  }

  def apply(region: String, bucketName: String, pathPrefix: String): S3Repository = S3Repository(SerializableCredentialsProvider[EnvironmentVariableCredentialsProvider], region, bucketName, pathPrefix)

  def apply(bucketName: String, pathPrefix: String = ".jar-cache"): S3Repository = {
    S3Repository(SerializableCredentialsProvider[EnvironmentVariableCredentialsProvider], SerializableRegionProvider[AwsEnvVarOverrideRegionProvider], bucketName, pathPrefix)
  }

}


trait SerializableCredentialsProvider extends AWSCredentialsProvider with Serializable
object SerializableCredentialsProvider {
  def apply[T <: AWSCredentialsProvider](implicit ct: ClassTag[T]): SerializableCredentialsProvider = new SerializableCredentialsProvider {
    @transient private lazy val instance = ct.runtimeClass.newInstance().asInstanceOf[AWSCredentialsProvider]
    override def refresh(): Unit = instance.refresh()
    override def getCredentials: AWSCredentials = instance.getCredentials
  }
}

trait SerializableRegionProvider extends AwsRegionProvider with Serializable
object SerializableRegionProvider {
  def apply[T <: AwsRegionProvider](implicit ct: ClassTag[T]): SerializableRegionProvider = new SerializableRegionProvider {
    @transient private lazy val instance = ct.runtimeClass.newInstance().asInstanceOf[AwsRegionProvider]
    override def getRegion: String = instance.getRegion
  }
}
