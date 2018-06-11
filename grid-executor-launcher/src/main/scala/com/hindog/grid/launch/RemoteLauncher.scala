package com.hindog.grid.launch

import com.hindog.grid.{ClasspathUtils, GridConfig, GridExecutor}
import com.hindog.grid.repo._
import com.typesafe.scalalogging.Logger
import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner

import scala.collection.{mutable, Iterable}
import scala.collection.mutable.ListBuffer

import java.io.File
import java.lang.management.ManagementFactory
import java.net.URI
import java.nio.file.Paths
import java.util.Properties
import java.util.concurrent.Callable

trait RemoteLauncher[C] {

  type Repr

  import RemoteLauncher._

  /**
    * Build remote submit command
    */
//  def submitCommand(args: Array[String]): Array[String]
  def buildProcess(args: Array[String]): ProcessBuilder

  protected def isSubmitted: Boolean
  protected def shellCommand: Iterable[String]
  protected def confGetOption(conf: C, key: String): Option[String]
  protected def confSetIfMissing(conf: C, key: String, value: String): C

  @transient protected val arguments: ListBuffer[ArgumentBuilder[Repr, C]] = mutable.ListBuffer[ArgumentBuilder[Repr, C]]()

  protected def arg(submitArg: String, confKey: String): Unit = arguments += Argument[Repr, C](Option(submitArg), Option(confKey), None, (_, conf) => confGetOption(conf, confKey))
  protected def arg(submitArg: String, accessor: (Repr, C) => Option[String]): Unit = arguments += Argument[Repr, C](Option(submitArg), None, None, accessor)
  protected def flag(submitFlag: String, confKey: String): Unit = arguments += Argument[Repr, C](None, Option(confKey), Option(submitFlag), (_, conf) => confGetOption(conf, confKey).map(_.toLowerCase))
  protected def flag(submitFlag: String): Unit = arguments += Argument[Repr, C](None, None, Option(submitFlag), (_, _) => Some("true"))
  protected def flag(submitFlag: String, accessor: (Repr, C) => Boolean): Unit = arguments += Argument[Repr, C](None, None, Option(submitFlag), (instance, conf) => if (accessor(instance, conf)) Some("true") else None)

  protected def grid: GridConfig = RemoteLaunch.gridConfig(getClass.getSimpleName.stripSuffix("$"))

  protected def onGridConfig(grid: GridConfig): GridConfig = grid

  protected def repository: Option[Repository] = {
    // TODO: source props from configuration file
    val props = new Properties()

    RemoteLaunch.launchArgs.jarCacheRepositoryClass.toOption.map(cls => Class.forName(cls).getConstructor(classOf[Properties]).newInstance(props).asInstanceOf[Repository])
  }

  protected def configure(args: Array[String], conf: C): C = {
    if (!isSubmitted) {
      RemoteLaunch.launchArgs.conf.foldLeft(conf)((acc, cur) => confSetIfMissing(acc, cur._1, cur._2))
    } else conf
  }

  protected def loadDefaults: Boolean = RemoteLaunch.launchArgs.loadDefaults()

  protected def mainClass: String = getClass.getName.stripSuffix("$")
  
  @transient lazy val applicationJar: String = {
    import scala.collection.JavaConverters._
    val result = new FastClasspathScanner(this.getClass.getName).ignoreFieldVisibility().ignoreMethodVisibility().suppressMatchProcessorExceptions().scan(10)

    val classInfoMap = result.getClassNameToClassInfo.asScala.filter(_._1 == this.getClass.getName)
    if (classInfoMap.isEmpty) throw new IllegalStateException(s"Unable to find classpath jar containing main-class: $mainClass")
    else {
      classInfoMap.head._2.getClasspathElementFile.toString
    }
  }

  protected def clusterClasspath: Iterable[String] = {
    val repo = repository
    val classpath = clusterClasspathFilter(ClasspathUtils.listCurrentClasspath.flatMap(u => Resource.parse(u.toURI)))
    classpath.map(cp => repo.flatMap(r => Option(r.resolve(cp))).getOrElse(cp.uri)).map(_.toString)
  }

  protected def clusterClasspathFilter: Iterable[Resource] => Iterable[Resource] = identity


  def main(args: Array[String]): Unit = {
    /*
      Detect if we are running via submit command, if so, run as normal, otherwise invoke remote launch...
     */
    if (isSubmitted) {
      run(args)
    } else {
      val repo = repository
      val gridConfig = grid.ifDefinedThen(repo)((g, repo) => g.addStartupHook(new SyncRepositoryHook(repo)))
                           .withInheritedSystemPropertiesFilter(_.startsWith("grid."))

      val retCode = GridExecutor.withInstance(gridConfig) { executor =>
        val task = executor.submit(new Callable[Int] with Serializable {
          override def call(): Int = {
            import scala.collection.JavaConverters._

            val logger = Logger(mainClass)
            logger.info(s"Running $mainClass remotely under process: ${ManagementFactory.getRuntimeMXBean.getName}")

            val builder = buildProcess(args)
            logger.info(s"Submit Command: \n\n" + builder.command().asScala.mkString(" ") + "\n")

            builder.environment().put(submitEnvFlag, "true")
            val process = builder.inheritIO().start()
            val ret = process.waitFor()
            ret
          }
        })

        try {
          task.get()
        } finally {
          // pause a bit to wait for StdOut/StdErr streams
          Thread.sleep(1000)
        }
      }

      System.exit(retCode)
    }
  }

  def run(args: Array[String])
}

object RemoteLauncher {

  val submitEnvFlag = "GRID_EXECUTOR_SUBMIT"

  type ArgumentBuilder[T, C] = (T, C) => Iterable[String]

  case class Argument[T, C](submitArg: Option[String], confKey: Option[String], flag: Option[String], accessor: (T, C) => Option[String]) extends ((T, C) => Iterable[String]) with Serializable {
    def apply(instance: T, conf: C): Iterable[String] = {
      accessor(instance, conf).map(value =>
        // if we have a flag, then we have slightly different behavior--
        // we pass the flag argument with no value
        if (flag.isDefined) {
          if (accessor(instance, conf).map(_.toLowerCase).contains("true")) {
            flag.toIterable
          } else {
            Iterable.empty
          }
        } else {
          submitArg.fold(Iterable.empty[String])(arg => Iterable(arg, value))
        }
      ).getOrElse(Iterable.empty)
    }
  }
}