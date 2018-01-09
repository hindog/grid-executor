package com.hindog.grid.spark

import com.hindog.grid.GridConfig
import org.scalatest.{Matchers, WordSpecLike}

import scala.collection._

import java.io.File

/**
  * Created by Aaron Hiniker (ahiniker@atomtickets.com) 
  * 12/21/17
  * Copyright (c) Atom Tickets, LLC
  */
class SparkRunnerTest extends WordSpecLike with Matchers {

  object TestRunner extends SparkRunner {
    conf.set("spark.master", "yarn")
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.driver.extraLibraryPath", "/extraPath")
    conf.set("spark.driver.extraJavaOptions", "-Dextra.opt=true -Dextra.opt2=true")
    conf.set("spark.driver.cores", "4")
    conf.set("spark.driver.supervise", "true")
    conf.set("spark.submit.verbose", "true")
    conf.set("spark.yarn.tags", "tag1")
    
    override def grid: GridConfig = GridConfig("test")
    override def run(args: Array[String]): Unit = ???
  }

  "SparkRunner" should {
    "use conf settings for driver arguments" in {
      val cmd = TestRunner.submitCommand(Array("arg1", "arg2"))
      val appJar = System.getProperty("java.class.path").split(File.pathSeparator).head
      val expected = Array(
        "/bin/bash",
        "spark-submit",
        "--verbose",
        "--class", s"com.hindog.grid.spark.SparkRunnerTest$$TestRunner",
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

      assertResult(expected)(cmd.toSeq)
    }
  }
}
