ghpages.settings

git.remoteRepo := "git@github.com:hindog/grid-executor.git"

enablePlugins(SiteScaladocPlugin)

val jcloudsVersion = "2.0.1"
val sparkVersion = "2.1.1"

lazy val commonSettings = Seq(
	name := "grid-executor",
	organization := "com.hindog.grid",
	scalaVersion := "2.11.11",
	crossScalaVersions := Seq("2.11.11"),
	releaseCrossBuild := true,
	releaseVersionBump := sbtrelease.Version.Bump.Bugfix,
	releasePublishArtifactsAction := PgpKeys.publishSigned.value,
	javacOptions ++= Seq("-Xlint:unchecked", "-source", "1.7", "-target", "1.7"),
	scalacOptions ++= Seq("-Xlint:_", "-Xfatal-warnings", "-target:jvm-1.7", "-feature", "-language:_"),
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

lazy val root = Project(
	id = "grid-executor",
	base = file(".")
).aggregate(core, awsS3, hadoop2, spark2, examples).settings(commonSettings)

lazy val core = project.in(file("grid-executor")).settings(commonSettings ++ Seq(
    moduleName := "grid-executor-core",
		libraryDependencies ++= Seq(
			"com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
			"commons-io" % "commons-io" % "2.1",
			"com.twitter" %% "chill" % "0.9.0",
			"com.google.guava" % "guava" % "19.0",
			"org.apache.xbean" % "xbean-asm5-shaded" % "4.5",
			"org.gridkit.lab" % "nanocloud" % "0.8.11",
			"org.slf4j" % "slf4j-log4j12" % "1.7.25" % "test",
			"org.scalatest" %% "scalatest" % "3.0.3" % "test"
		)
  )
)

lazy val awsS3 = project.in(file("grid-executor-s3")).settings(commonSettings ++ Seq(
    moduleName := "grid-executor-s3",
    libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.126"
  )
).dependsOn(core)

lazy val hadoop2 = project.in(file("grid-executor-hadoop2")).settings(commonSettings ++ Seq(
    moduleName := "grid-executor-hadoop2",
		libraryDependencies ++= Seq(
			"org.apache.hadoop" % "hadoop-client" % "2.7.2"
    )
  )
).dependsOn(core)

lazy val spark2 = project.in(file("grid-executor-spark2")).settings(commonSettings ++ Seq(
    moduleName := "grid-executor-spark2",
    libraryDependencies ++= Seq(
			"org.aspectj" % "aspectjrt" % "1.8.9" % "provided",
			"org.apache.spark" %% "spark-core" % sparkVersion % "provided",
			"org.apache.spark" %% "spark-repl" % sparkVersion % "provided"
		)
	)
).dependsOn(core)

lazy val examples = project.in(file("grid-executor-examples")).dependsOn(core, hadoop2, awsS3, spark2).settings(commonSettings ++ Seq(
    moduleName := "grid-executor-examples",
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-log4j12" % "1.7.25",
			"org.scala-lang" % "scala-compiler" % scalaVersion.value,
      "org.apache.jclouds" % "jclouds-core" % jcloudsVersion,
      "org.apache.jclouds.driver" % "jclouds-log4j" % jcloudsVersion,
      "org.apache.jclouds.driver" % "jclouds-sshj" % jcloudsVersion,
      "org.apache.jclouds.provider" % "aws-ec2" % jcloudsVersion,
			"com.amazonaws" % "aws-java-sdk-s3" % "1.11.126",
			"org.apache.hive" % "hive" % "2.1.1" % "provided",
			"org.apache.spark" %% "spark-core" % sparkVersion % "compile",
			"org.apache.spark" %% "spark-repl" % sparkVersion % "compile"
		)
  )
)