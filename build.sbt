enablePlugins(GhpagesPlugin)
enablePlugins(SiteScaladocPlugin)

git.remoteRepo := "git@github.com:hindog/grid-executor.git"

val jcloudsVersion = "2.0.1"
val sparkVersion = "2.4.4"
val hadoopVersion = "2.8.5"

ThisBuild / scalaVersion := "2.11.12"

lazy val commonSettings = Seq(
	name := "grid-executor",
	organization := "com.hindog.grid",
	scalaVersion := "2.11.12",
	crossScalaVersions := Seq("2.11.12", "2.12.12"),
	libraryDependencies ++= Seq(
		"org.scalatest" %% "scalatest" % "3.0.3" % "test"
	),
	releaseCrossBuild := true,
	releaseVersionBump := sbtrelease.Version.Bump.Bugfix,
	releasePublishArtifactsAction := PgpKeys.publishSigned.value,
	javacOptions ++= Seq("-Xlint:unchecked", "-source", "1.8", "-target", "1.8"),
	scalacOptions ++= Seq("-Xlint:_", "-target:jvm-1.8", "-feature", "-language:_"),
	testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oF"),
	autoAPIMappings := true,
	publishMavenStyle := true,
	pomIncludeRepository := { _ => false },
	parallelExecution in Test := false,
	publishArtifact in Test := false,
	publishTo := {
		val nexus = "https://oss.sonatype.org/"
		if (isSnapshot.value)
			Some("snapshots" at nexus + "content/repositories/snapshots")
		else
			Some("releases"  at nexus + "service/local/staging/deploy/maven2")
	},
	pomExtra := (
		<url>https://github.com/hindog/grid-executor</url>
			<licenses>
				<license>
					<name>Apache 2</name>
					<url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
					<distribution>repo</distribution>
					<comments>A business-friendly OSS license</comments>
				</license>
			</licenses>
			<scm>
				<url>git@github.com:hindog/grid-executor.git</url>
				<connection>scm:git:git@github.com:hindog/grid-executor.git</connection>
			</scm>
			<developers>
				<developer>
					<id>hindog</id>
					<name>Aaron Hiniker</name>
					<url>https://github.com/hindog/</url>
				</developer>
			</developers>
		)
)

lazy val `grid-executor` = Project(
	id = "grid-executor",
	base = file(".")
).settings(name := "root").aggregate(core, awsS3, launcher, hadoop2, spark2, examples)

lazy val core = project.in(file("grid-executor")).settings(commonSettings).settings(
		name := "grid-executor-core",
		libraryDependencies ++= Seq(
			"com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
			"commons-io" % "commons-io" % "2.1",
			"commons-codec" % "commons-codec" % "1.10",
			"org.apache.commons" % "commons-compress" % "1.17",
			"com.twitter" %% "chill" % "0.9.0",
			"com.google.guava" % "guava" % "19.0",
			"com.jsuereth" %% "scala-arm" % "2.0",
			"com.github.pathikrit" %% "better-files" % "3.5.0",
			"org.apache.xbean" % "xbean-asm5-shaded" % "4.5",
			"org.gridkit.lab" % "nanocloud" % "0.8.16",
			"com.github.igor-suhorukov" % "mvn-classloader" % "1.9",
			"io.github.classgraph" % "classgraph" % "4.8.90",
			"org.slf4j" % "slf4j-log4j12" % "1.7.25" % "test"
		)
  )

lazy val awsS3 = project.in(file("grid-executor-s3")).settings(commonSettings).settings(
		name := "grid-executor-s3",
    libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.358" % "provided"
  ).dependsOn(core)

lazy val launcher = project.in(file("grid-executor-launcher")).settings(commonSettings).settings(
	name := "grid-executor-launcher",
	libraryDependencies ++= Seq(
		"org.rogach" %% "scallop" % "3.1.2",
		"io.github.lukehutch" % "fast-classpath-scanner" % "2.21"
	)
).dependsOn(core)

lazy val hadoop2 = project.in(file("grid-executor-hadoop2")).settings(commonSettings).settings(
		name := "grid-executor-hadoop2",
		libraryDependencies ++= Seq(
			"org.apache.hadoop" % "hadoop-client" % hadoopVersion exclude("com.amazonaws", "aws-java-sdk")
    )
	).dependsOn(core, launcher)

lazy val spark2 = project.in(file("grid-executor-spark2")).settings(commonSettings).settings(
    name := "grid-executor-spark2",
    libraryDependencies ++= Seq(
			"org.aspectj" % "aspectjrt" % "1.8.9",
			"org.apache.spark" %% "spark-core" % sparkVersion,
			"org.apache.spark" %% "spark-repl" % sparkVersion
		)
	).dependsOn(core, launcher)

lazy val examples = project.in(file("grid-executor-examples")).settings(commonSettings).settings(
		name := "grid-executor-examples",
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-log4j12" % "1.7.25",
			"org.scala-lang" % "scala-compiler" % scalaVersion.value,
      "org.apache.jclouds" % "jclouds-core" % jcloudsVersion,
      "org.apache.jclouds.driver" % "jclouds-log4j" % jcloudsVersion,
      "org.apache.jclouds.driver" % "jclouds-sshj" % jcloudsVersion,
      //"org.apache.jclouds.provider" % "aws-ec2" % jcloudsVersion,
			"com.amazonaws" % "aws-java-sdk-s3" % "1.11.358",
//			"org.apache.hive" % "hive" % "2.3.0" % "provided",
			"org.apache.spark" %% "spark-core" % sparkVersion,
			"org.apache.spark" %% "spark-repl" % sparkVersion,
			"org.apache.spark" %% "spark-hive-thriftserver" % sparkVersion,
			"org.apache.spark" %% "spark-kubernetes" % sparkVersion,
			"org.apache.hadoop" % "hadoop-aws" % hadoopVersion
		)
  ).dependsOn(core, hadoop2, awsS3, spark2)
