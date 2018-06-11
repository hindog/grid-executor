package com.hindog.grid.spark

import com.hindog.grid.{GridConfig, Logging}
import org.apache.spark.SparkConf
import org.scalatest.{Matchers, WordSpecLike}

import scala.collection._
import scala.collection.JavaConverters._

import java.io.File

/**
  * Created by Aaron Hiniker (ahiniker@atomtickets.com) 
  * 12/21/17
  * Copyright (c) Atom Tickets, LLC
  */
class SparkLauncherTest extends WordSpecLike with Matchers with Logging {
  System.setProperty("grid.node", "local")
  
  object TestLauncher extends SparkLauncher {

    def appJar = applicationJar // expose this publicly for tests

    override def configure(args: Array[String], conf: SparkConf): SparkConf = {
      conf.set("spark.master", "yarn")
      conf.set("spark.driver.memory", "4g")
      conf.set("spark.driver.extraLibraryPath", "/extraPath")
      conf.set("spark.driver.extraJavaOptions", "-Dextra.opt=true -Dextra.opt2=true")
      conf.set("spark.driver.cores", "4")
      conf.set("spark.driver.supervise", "true")
      conf.set("spark.submit.verbose", "true")
      conf.set("spark.yarn.tags", "tag1")
    }

    override protected def loadDefaults: Boolean = false
    override def grid: GridConfig = GridConfig("test")
    override def run(args: Array[String]): Unit = ???
  }

  "SparkRunner" should {
    "use conf settings for driver arguments" in {
      val cmd = TestLauncher.buildProcess(Array("arg1", "arg2")).command().asScala
      val appJar = TestLauncher.appJar
      val expected = Array(
        "spark-submit",
        "--verbose",
        "--class", "com.hindog.grid.spark.SparkLauncherTest$TestLauncher",
        "--master", "yarn",
        "--driver-memory", "4g",
        "--driver-java-options", "-Dextra.opt=true -Dextra.opt2=true",
        "--driver-library-path", "/extraPath",
        "--driver-cores", "4",
        "--supervise",
        "--conf", "spark.yarn.tags=tag1",
        appJar,
        "arg1", "arg2"
      ).toSeq

      println("EXPECTED: " + expected.mkString(" "))
      println("ACTUAL:   " + cmd.mkString(" "))
      
      assertResult(expected)(cmd.toSeq)
    }
  }
}
