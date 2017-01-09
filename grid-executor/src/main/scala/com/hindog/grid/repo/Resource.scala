package com.hindog.grid
package repo

import java.io.File
import java.net.URI

import org.apache.commons.io.FileUtils

import scala.util.matching.Regex

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
case class Resource(baseUri: URI, path: String) extends Serializable {

  def fetch: File = {
    if (baseUri.getScheme.toLowerCase == "file") {
      new File(baseUri / path)
    } else {
      val tmp = File.createTempFile("jar-cache", ".jar")
      FileUtils.copyURLToFile((baseUri / path).toURL, tmp)
      tmp
    }
  }

  override def toString: String = uri.toString

  def uri = baseUri / path
}

object Resource extends Logging {

  /**
    * Regex used to extract a UUID from a url
    *
    * Minimum requirements are a 32 character, alpha-numeric and/or dash sequence.
    * (UUID.toString, MD5, SHA1, etc are all viable "UUID" patterns)
    *
    */
  protected val hashPattern: Regex = "(.*?)([A-Za-z0-9\\-]{36,}.*)".r

  /**
    * Parse a URI expecting some form of UUID/Hash digest in the URI's path based on the 'hashPattern' regex
    */
  def parse(uri: URI): Option[Resource] = {
    hashPattern.findFirstMatchIn(uri.toString).map(m => Resource(new URI(m.group(1)), m.group(2)))
  }
}

