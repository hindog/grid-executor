package com.hindog.grid
package hadoop

import com.hindog.grid.repo.{Resource, _}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import scala.concurrent.duration._
import scala.language.postfixOps

import java.io.{Externalizable, ObjectInput, ObjectOutput, File => JFile}
import java.util.{Date, Properties}
import better.files._

import java.net.URI

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
case class HDFSRepository(properties: Properties) extends Repository with Logging {

  import HDFSRepository._

  private val basePath = Option(properties.getProperty("base-dir")).getOrElse(defaultPath) match {
    case p if p.startsWith("~") => path(fs.getHomeDirectory.toUri.getPath, p.stripPrefix("~").stripPrefix("/"))
    case p if !p.startsWith("/") => path(fs.getHomeDirectory.toUri.getPath, p)
    case p => path(p)
  }

  private val replication = Option(properties.getProperty("replication")).map(_.toInt).getOrElse(1)
  private val cleanInterval = Option(properties.getProperty("clean-interval")).map(Duration.apply).getOrElse(defaultCleanInterval).toMillis
  private val maxLastAccessAge = Option(properties.getProperty("max-last-access-age")).map(Duration.apply).getOrElse(defaultMaxLastAccessAge).toMillis
  private val cleanupTimestampFile = path(basePath, ".last-clean-ts")

  @transient private lazy val fs = FileSystem.get(HadoopEnvironment.loadConfiguration())

  protected def path(base: String, parts: String*): Path = path(new Path(base), parts: _*)
  protected def path(base: Path, parts: String*): Path = parts.foldLeft(base)((acc, cur) => new Path(acc, cur))
  protected def resourcePath(filename: String, contentHash: String): Path = path(basePath, contentHash, contentHash + "-" + filename)

  override def contains(resource: Resource): Boolean = {
    logger.info("Checking if exists: " + resourcePath(resource.filename, resource.contentHash) + "? " + fs.exists(resourcePath(resource.filename, resource.contentHash)))
    fs.exists(resourcePath(resource.filename, resource.contentHash))
  }

  override def get(filename: String, contentHash: String): Resource = {
    val resPath = resourcePath(filename, contentHash)
    new HadoopResource(filename, contentHash, fs.getFileStatus(resPath).getLen, resPath, fs)
  }

  override def put(res: Resource): Resource = {
    val resPath = resourcePath(res.filename, res.contentHash)
    if (contains(res)) {
      new HadoopResource(res.filename, res.contentHash, fs.getFileStatus(resPath).getLen, resPath, fs)
    } else {
      fs.mkdirs(resPath.getParent)

      val ret = for {
        in <- res.inputStream.buffered.autoClosed
        out <- fs.create(resPath, replication.toShort).autoClosed
      } yield in.pipeTo(out)

      ret.get()
      val len = fs.getFileStatus(resPath).getLen
      require(len == res.contentLength, s"Content length mismatch for $resPath")

      logger.info(s"Uploaded $res (${res.contentLength / 1024} Kb) to $resPath")

      new HadoopResource(res.filename, res.contentHash,len, resPath, fs)
    }
  }


  def cleanup(): Unit = {
    logger.info(s"Cleaning jar cache files in $basePath...")
    // update timestamp
    IOUtils.write(System.currentTimeMillis().toString, fs.create(cleanupTimestampFile))

    // remove all files that have not been accessed since 'maxLastAccessAge' ago
    val files = fs.listFiles(basePath, true)
    Stream.continually(files).takeWhile(_.hasNext).map(_.next()).foreach(file => {
      if (System.currentTimeMillis() - file.getAccessTime > maxLastAccessAge) {
        logger.debug(s"Removing cached jar ${file.getPath} (last accessed: ${new Date(file.getAccessTime)})")
        fs.delete(file.getPath, true)
      }
    })
  }

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
        case ex: Exception => logger.warn(s"Exception while attempting to clean repository cache directory $basePath.  ${cleanupTimestampFile.toString} possibly corrupt and needs to be removed?", ex)
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
    override def writeExternal(out: java.io.ObjectOutput): Unit = write(out)
    override def readExternal(in: java.io.ObjectInput): Unit = readFields(in)
  }
}
