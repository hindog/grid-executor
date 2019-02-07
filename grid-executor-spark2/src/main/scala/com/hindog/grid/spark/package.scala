package com.hindog.grid

import org.apache.spark.SparkConf

package object spark {

  implicit class SparkConfOps(c: SparkConf) {
    def set(key: String, value: Boolean): SparkConf = c.set(key, value.toString)
    def set(key: String, value: Long): SparkConf = c.set(key, value.toString)
    def set(key: String, value: Double): SparkConf = c.set(key, value.toString)

    def setIfMissing(key: String, value: Boolean): SparkConf = c.setIfMissing(key, value.toString)
    def setIfMissing(key: String, value: Long): SparkConf = c.setIfMissing(key, value.toString)
    def setIfMissing(key: String, value: Double): SparkConf = c.setIfMissing(key, value.toString)

    def append(key: String, value: String): SparkConf = c.set(key, s"${c.getOption(key).map(existing => s"$existing $value").getOrElse(value)}".trim)
  }

}
