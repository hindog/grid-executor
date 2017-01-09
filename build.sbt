ghpages.settings

git.remoteRepo := "git@github.com:hindog/grid-executor.git"

enablePlugins(SiteScaladocPlugin)

lazy val root = Project(
	id = "root",
	base = file(".")
).settings(
	Seq(
		organization := "com.hindog.grid",
		moduleName := "grid-executor",
		name := "grid-executor",
		scalaVersion := "2.11.8",
		crossScalaVersions := Seq("2.10.6", "2.11.8"),
		releaseCrossBuild := true,
		releaseVersionBump := sbtrelease.Version.Bump.Bugfix,
		releasePublishArtifactsAction := PgpKeys.publishSigned.value,
		javacOptions ++= Seq("-Xlint:unchecked", "-source", "1.7", "-target", "1.7"),
		scalacOptions ++= Seq("-unchecked", "-deprecation", "-target:jvm-1.7", "-feature", "-language:_"),
		testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oF"),
		libraryDependencies ++= Seq(
			"org.clapper" % "grizzled-slf4j_2.11" % "1.3.0",
			"com.twitter" %% "chill" % "0.9.0",
			"org.apache.xbean" % "xbean-asm5-shaded" % "4.5",
			"commons-io" % "commons-io" % "2.1",
			"org.gridkit.lab" % "nanocloud" % "0.8.11",
			"org.scalatest" %% "scalatest" % "2.2.6" % "test"
		),
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
)