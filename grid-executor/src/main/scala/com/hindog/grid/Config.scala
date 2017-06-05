package com.hindog.grid

import scala.collection.JavaConverters._
import scala.collection._

/**
  * Created by atom-aaron on 6/3/17
  */
case class Config private (map: Map[String, String] = Map.empty) {
  def set(key: String, value: String): Config = copy(map = map + (key -> value))
  def set(key: String, value: Double): Config = copy(map = map + (key -> value.toString))
  def set(key: String, value: Long): Config = copy(map = map + (key -> value.toString))
  def set(key: String, value: Boolean): Config = copy(map = map + (key -> value.toString))

  def append(key: String, value: String): Config = copy(map = map + (key -> (map.get(key).map(_ + " ").getOrElse("") + value)))
  
  def get(key: String, default: String): String = map.getOrElse(key, default)
  def getBoolean(key: String, default: Boolean): Boolean = map.get(key).map(_.toBoolean).getOrElse(default)
  def getDouble(key: String, default: Double): Double = map.get(key).map(_.toDouble).getOrElse(default)
  def getLong(key: String, default: Long): Long = map.get(key).map(_.toLong).getOrElse(default)
  def getOption(key: String): Option[String] = map.get(key)

  def getAll: Iterable[(String, String)] = map
  def setAll(values: Iterable[(String, String)]): Config = copy(map = map ++ values.toMap)
  
}

object Config {
  def apply(prefixes: String*): Config = {
    new Config(System.getProperties.asScala.filter(_._1.contains(".")).filter(kv => prefixes.contains(kv._1.split("\\.").head)).toMap)
  }
}
