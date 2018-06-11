package com.hindog.grid
package hadoop

import com.hindog.grid.hadoop.HDFSRepository.SerializableConfiguration

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.net.URI
import java.util.{Date, Properties}
import com.hindog.grid.repo.{Repository, Resource}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import scala.concurrent.duration._
import scala.language.postfixOps

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
case class HDFSRepository(properties: Properties) extends Repository with Logging {

  import HDFSRepository._

  private val hdfsPath = Option(properties.getProperty("base-dir")).getOrElse(defaultPath)
  private val cleanInterval = Option(properties.getProperty("clean-interval")).map(Duration.apply).getOrElse(defaultCleanInterval).toMillis
  private val maxLastAccessAge = Option(properties.getProperty("max-last-access-age")).map(Duration.apply).getOrElse(defaultMaxLastAccessAge).toMillis

  @transient private lazy val fs = FileSystem.get(HadoopEnvironment.loadConfiguration())

  @transient private lazy val path = {
    if (!hdfsPath.startsWith("/")) {
      new Path(fs.getHomeDirectory.toUri.getPath, hdfsPath)
    } else {
      new Path(hdfsPath)
    }
  }

  @transient protected lazy val cleanupTimestampFile = new Path(path, ".last-clean-ts")

  override def uri: URI = new URI(fs.getUri + path.toString)

  override def contains(resource: Resource): Boolean = fs.exists(new Path(path, resolve(resource).path))

  override def upload(resource: Resource): Resource = {
    val targetResource = resolve(resource)

    logger.info(s"Uploading ${resource.uri} to ${targetResource.uri}")
    fs.mkdirs(path)
    fs.copyFromLocalFile(new Path(resource.uri), new Path(path, targetResource.path))
    targetResource
  }

  def cleanup(): Unit = {
    logger.info(s"Cleaning jar cache files in $path...")
    // update timestamp
    IOUtils.write(System.currentTimeMillis().toString, fs.create(cleanupTimestampFile))

    // remove all files that have not been accessed since 'maxLastAccessAge' ago
    val files = fs.listFiles(path, true)
    Stream.continually(files).takeWhile(_.hasNext).map(_.next()).foreach(file => {
      if (System.currentTimeMillis() - file.getAccessTime > maxLastAccessAge) {
        logger.debug(s"Removing cached jar ${file.getPath} (last accessed: ${new Date(file.getAccessTime)})")
        fs.delete(file.getPath, true)
      }
    })
  }


  override def resolve(resource: Resource) = Resource(uri, resource.path.replaceAll("/", "-"))

  /*
    Seems like on-close is an OK place to check for stale jars and clean them up
   */
  override def close(): Unit = {
    if (fs.exists(cleanupTimestampFile)) {
      try {
        val timestamp = IOUtils.toString(fs.open(cleanupTimestampFile)).toLong
        logger.info(s"Last jar cache auto-clean: ${new Date(timestamp)}")
        if (System.currentTimeMillis() - timestamp > cleanInterval) {
          cleanup()
        }
      } catch {
        case ex: Exception => logger.warn(s"Exception while attempting to clean repository cache directory $path.  ${cleanupTimestampFile.toString} possibly corrupt and needs to be removed?", ex)
      }
    } else {
      cleanup()
    }

    fs.close()
  }
}

object HDFSRepository {
  private val defaultPath = ".jar-cache"
  private val defaultCleanInterval = 24 hours
  private val defaultMaxLastAccessAge = 30 days

  // TODO: allow custom Hadoop configuration and use this for a serializable copy
  case class SerializableConfiguration(var conf: Configuration) extends Configuration(conf) with Externalizable {
    def this() = this(new Configuration(true))
    override def writeExternal(out: ObjectOutput): Unit = write(out)
    override def readExternal(in: ObjectInput): Unit = readFields(in)
  }
}
