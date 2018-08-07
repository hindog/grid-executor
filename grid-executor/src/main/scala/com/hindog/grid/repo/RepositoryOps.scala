package com.hindog.grid.repo

import scala.concurrent.duration._
import scala.util.matching.Regex

import java.io._
import java.time.Instant
import java.util.Properties

trait RepositoryOps {
  protected def properties: Properties


  trait PropertyExtractor[T] {
    def apply(t: String): Option[T]
  }

  implicit object IntExtractor extends PropertyExtractor[Int] {
    override def apply(t: String): Option[Int] = Option(properties.getProperty(t)).map(_.toInt)
  }

  implicit object StringExtractor extends PropertyExtractor[String] {
    override def apply(t: String): Option[String] = Option(properties.getProperty(t))
  }

  implicit object DurationExtractor extends PropertyExtractor[Duration] {
    override def apply(t: String): Option[Duration] = Option(properties.getProperty(t)).map(Duration.apply)
  }
  
  protected def prop[T](key: String)(implicit extractor: PropertyExtractor[T]): Option[T] = extractor(key)
  protected def prop[T](key: String, default: => T)(implicit extractor: PropertyExtractor[T]): T = extractor(key).getOrElse(default)

//  protected def toString(t: T): String
//  protected def lift(s: String): T
  protected val extractPath: Regex = "([A-Za-z0-9\\-]{40})/[A-Za-z0-9\\-]{40}\\-(.*)".r

//  protected def writeMetadata[T](key: String, entries: T): Unit
//  protected def readMetadata: Option[Json]

//  protected def resourcePath(res: Resource): String = res.contentHash / (res.contentHash + "-" + res.filename)
//  protected def resourcePath(filename: String, contentHash: String): String = contentHash / (contentHash + "-" + filename)

  protected lazy val cleanInterval = prop[Duration]("clean-interval").getOrElse(24 hours)
  protected lazy val maxAccessLastAge = prop[Duration]("max-last-access-age").getOrElse(30 days)
  
//  implicit class StringPathOps(s: String) {
//    def /(that: String): String = s.stripSuffix("/") + "/" + that.stripPrefix("/")
//  }
//
//  implicit class PathOps(t: T) {
//    def /(that: String): T = lift(t.toString.stripSuffix("/") + "/" + that.stripPrefix("/"))
//    def /(that: T): T = lift(t.toString / that.toString)
//  }

}
