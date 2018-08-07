package com.hindog.grid

import better.files.File
import com.hindog.grid.repo.Resource
import org.scalatest._

import java.nio.file.Paths

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
class FileResourceTest extends WordSpecLike with Matchers {

  val lastModified = 1533679888000L

  /**
    * This is used to create a temporary copy of a folder where the last modified times
    * will be set to a well-known value so that we can compute a stable content hash for testing.
    */
  def copyFolderForTest(path: String): File = {
    val source = File(getClass.getResource(path))
    val dest = File.newTemporaryDirectory()
    val out = dest / path.stripPrefix("/")
    out.toJava.mkdirs()
    source.copyToDirectory(dest)
    out.listRecursively().foreach(f => f.toJava.setLastModified(lastModified))
    out
  }

  def copyFileForTest(path: String): File = {
    val source = File(getClass.getResource(path))
    val dest = File.newTemporaryDirectory()
    val out = source.copyToDirectory(dest)
    out.toJava.setLastModified(lastModified)
    out
  }

  "FileResource" should {

    "archive directory" in {
      val res = Resource.dir(copyFolderForTest("/test-folder"))
      
      assertResult("2397f6f14d08111f8aa980cd56011bff331334fa")(res.contentHash)
      assertResult(32034)(res.contentLength)
      assertResult("test-folder.jar")(res.filename)
    }

    "archive file" in {
      val file = copyFileForTest("/test-folder/apple.jpg")
      val res = Resource.file(file.name, file)
      assertResult("f375bb23c53a96578ec240c646de2e6f8407c86d")(res.contentHash)
      assertResult(31879)(res.contentLength)
      assertResult("apple.jpg")(res.filename)
    }



  }


}
