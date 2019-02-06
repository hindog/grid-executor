package com.hindog.grid.launch

import com.hindog.grid.{ClasspathUtils, GridConfig, GridExecutor}
import com.hindog.grid.repo.{Repository, Resource}
import com.typesafe.scalalogging.Logger
import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner

import scala.collection.{mutable, Iterable, Set}
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

import java.lang.management.ManagementFactory
import java.util.concurrent.Callable

trait Launcher[LC <: Launcher.Config] {

  type Conf
  
  protected def configureLaunch(config: LC): LC = config

  protected[grid] def createLaunchConfig(args: Array[String] = Array.empty): LC

  def isSubmitted: Boolean

  def main(args: Array[String]): Unit = {
    /*
      Detect if we are running via submit command, if so, run as normal, otherwise invoke remote launch...
     */
    if (isSubmitted) {
      run(args)
    } else {
      val launcher = createLaunchConfig(args) //configureLaunch(launcher)
      
      val gridConfig = launcher.gridConfig
        .withInheritedSystemPropertiesFilter(_.startsWith("grid."))
        .withSystemProperty(Launcher.submitPropFlag, "true")

      val retCode = GridExecutor.withInstance(gridConfig) { executor =>
        val task = executor.submit(new Callable[Int] with Serializable {
          override def call(): Int = {

            val logger = Logger(launcher.mainClass)

            logger.info(s"Running ${launcher.mainClass} remotely under process: ${ManagementFactory.getRuntimeMXBean.getName}")

            val builder = launcher.buildProcess()
            logger.info(s"Submit Command: \n\n" + builder.command().asScala.mkString(" ") + "\n")

            val env = builder.environment()
            env.put(Launcher.submitEnvFlag, "true")
            
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

object Launcher {

  val submitEnvFlag   = "GRID_EXECUTOR_SUBMIT"
  val submitPropFlag   = "grid.executor.submit"

  def isRemoteLaunch: Boolean = "true" == System.getenv(submitEnvFlag) || "true" == System.getProperty(submitPropFlag)

  abstract class Config extends Serializable {

    type Conf

    private[grid] var args                    : Array[String]                                = Array.empty
    private[grid] var mainClass               : String                                       = "[No main class]"
    private[grid] var shellCommand            : Seq[String]                                  = Seq.empty
    private[grid] var applicationJar          : Option[String]                               = None
    private[grid] var repository              : Option[Repository]                           = None
    private[grid] val gridConfigs             : mutable.ListBuffer[GridConfig => GridConfig] = ListBuffer.empty
    private[grid] val clusterClasspathFilters : mutable.ListBuffer[Iterable[Resource] => Iterable[Resource]] = ListBuffer.empty
    private[grid] val classpathFilters: mutable.ListBuffer[Iterable[Resource] => Iterable[Resource]] = ListBuffer.empty
    private[grid] val remoteHooks             : mutable.ListBuffer[this.type => this.type]   = ListBuffer.empty
    private[grid] var conf                    : Conf

    def buildProcess(): ProcessBuilder

    protected def getConfValue(key: String): Option[String]

    protected val arguments: ListBuffer[ArgumentBuilder] = mutable.ListBuffer[ArgumentBuilder]()

    protected def arg(submitArg: String, confKey: String): Unit = arguments += Argument(submitArg, () => getConfValue(confKey), Some(confKey))
    protected def arg(submitArg: String, accessor: () => Option[String]): Unit = arguments += Argument(submitArg, accessor)
    protected def flag(submitFlag: String, confKey: String): Unit = arguments += Flag(submitFlag, () => getConfValue(confKey).map(_.toLowerCase.toBoolean), Some(confKey))
    protected def flag(submitFlag: String, accessor: () => Option[Boolean]): Unit = arguments += Flag(submitFlag, accessor)

    def getArgs: Array[String] = args
    def getRepository: Option[Repository] = repository
    def getMainClass: String = mainClass
    def getShellCommand: Seq[String] = shellCommand
    def getConf: Conf = conf

    def setConf(conf: Conf): this.type  = { this.conf = conf; this }

    def withMainClass(cls: String): this.type = { mainClass = cls; this }
    def withArgs(args: Array[String]): this.type = { this.args = args; this }
    def withConf(f: Conf => Conf): this.type = { this.conf = f(this.conf); this }
    def withGridConfig(f: GridConfig => GridConfig): this.type = { gridConfigs += f; this }
    def withClusterClasspathFilter(f: Iterable[Resource] => Iterable[Resource]): this.type = { clusterClasspathFilters += f; this }
    def withClasspathFilter(f: Iterable[Resource] => Iterable[Resource]): this.type = { classpathFilters += f; this }
    def withRepository(repo: Option[Repository]): this.type = { repository = repo; this }
    def withApplicationJar(jar: Option[String]): this.type = { applicationJar = jar; this }
    def withShellCommand(cmd: Seq[String]): this.type = { shellCommand = cmd; this }
    def withRemoteHook(f: this.type => this.type): this.type = { remoteHooks += f; this }

    def gridConfig: GridConfig = gridConfigs.foldLeft(GridConfig(mainClass.split("\\.").last.stripSuffix("$")))((acc, cur) => cur(acc))

    def resolveApplicationJar: String = applicationJar.getOrElse {
      import scala.collection.JavaConverters._
      val result = new FastClasspathScanner(mainClass).ignoreFieldVisibility().ignoreMethodVisibility().suppressMatchProcessorExceptions().scan(10)

      val classInfoMap = result.getClassNameToClassInfo.asScala.filter(_._1 == mainClass)
      if (classInfoMap.isEmpty) throw new IllegalStateException(s"Unable to find classpath jar containing main-class: $mainClass")
      else {
        val file = classInfoMap.head._2.getClasspathElementFile
        repository.map(repo => repo.put(Resource.file(file))).getOrElse(file).toString
      }
    }

    trait ArgumentBuilder extends (() => Iterable[String]) with Serializable {
      def arg: String
      def confKey: Option[String]
    }

    case class Argument(arg: String, accessor: () => Option[String], confKey: Option[String] = None) extends ArgumentBuilder {
      def apply(): Iterable[String] = {
        accessor().map(value => Iterable(arg, value)).getOrElse(Iterable.empty)
      }
    }

    case class Flag(arg: String, accessor: () => Option[Boolean], confKey: Option[String] = None) extends ArgumentBuilder {
      def apply(): Iterable[String] = {
        if (accessor().contains(true)) Iterable(arg)
        else Iterable.empty
      }
    }

    protected def applicationClasspath: Array[Resource] = ClasspathUtils.listCurrentClasspath.map(Resource.url)

    protected def argumentConfKeys: Set[String] = arguments.flatMap(_.confKey.toSeq).toSet

  }

}