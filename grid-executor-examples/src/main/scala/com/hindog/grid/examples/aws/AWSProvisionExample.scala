package com.hindog.grid.examples.aws

import com.hindog.grid.{GridConfig, GridExecutor, Logging, RemoteNodeConfig}
import org.jclouds.aws.ec2.compute.AWSEC2TemplateOptions.Builder._
import org.jclouds.compute.ComputeServiceContext
import org.jclouds.logging.log4j.config.Log4JLoggingModule
import org.jclouds.sshj.config.SshjSshClientModule

import scala.collection._
import scala.collection.JavaConversions._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import java.io.File
import java.lang.management.ManagementFactory
import java.util.Properties

/*
 * Demonstrates how you could use JClouds API to automatically provision grid nodes for use with grid-executor
 */
object AWSProvisionExample extends App with Logging {

  val user = System.getenv("AWS_ACCESS_KEY_ID")
  val password = System.getenv("AWS_SECRET_ACCESS_KEY")
  val region = "us-west-2"

  import org.jclouds.ContextBuilder

  val config = new Properties()

  // required to work-around: https://issues.apache.org/jira/browse/JCLOUDS-1269
  config.setProperty("jclouds.regions", region)

  val context = ContextBuilder.newBuilder("aws-ec2").endpoint(s"https://ec2.$region.amazonaws.com").credentials(user, password).modules(Seq(new Log4JLoggingModule(), new SshjSshClientModule)).overrides(config).buildView(classOf[ComputeServiceContext])
  val cs = context.getComputeService

  try {
    val nodeOptions = cs.templateBuilder().locationId(region).imageId(s"$region/ami-339e8152").hardwareId("m4.xlarge").options(
      blockUntilRunning(true)
      .blockOnPort(22, 600)
      .tags(Set("ahiniker", "grid-test"))
    )

    logger.info("creating nodes...")
    val nodes = cs.createNodesInGroup("default", 2, nodeOptions.build())
    Runtime.getRuntime.addShutdownHook(new Thread() {
      logger.info("shutting down nodes...")
      val shutdowns = Future.sequence(nodes.map(n => Future(cs.destroyNode(n.getProviderId))))
      Await.result(shutdowns, 120 seconds)
    })

    logger.info("created nodes: \n\t" + nodes.map(_.toString).mkString("\n\t"))
    nodes.foreach(n => logger.info("\t" + n))


    try {

      val run = Future {
        logger.info("starting grid...")
        val gridConfig = GridConfig("test").withNodes(nodes.map(nm => RemoteNodeConfig(nm.getPrivateAddresses.head, nm.getId).withSSHKey(new File("~/.ssh/devKeyPair.pem")).withSSHAccount("ec2-user")).toSeq: _*)
        GridExecutor.withInstance(gridConfig) { grid =>
          (0 until 10).foreach(i => {
            logger.info(s"executing task $i...")
            grid.submit(new Runnable with Serializable {
              override def run(): Unit = {
                Thread.sleep(5000)
                logger.info(s"executed task $i on: " + ManagementFactory.getRuntimeMXBean().getName())
                Thread.sleep(1000)
              }
            })
          })
        }
      }

      Await.result(run, 10 minutes)
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
  } finally {
    context.close()
  }
}
