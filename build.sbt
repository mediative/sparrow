import sbt.Keys._

import scalariform.formatter.preferences._

addCommandAlias("format", ";compile:scalariformFormat;test:scalariformFormat")

enablePlugins(GitVersioning)
git.useGitDescribe := true

lazy val commonSettings = Seq(
  organization       := "com.mediative",
  scalaVersion       := "2.10.5",
  crossScalaVersions := Seq("2.10.5", "2.11.7"),
  licenses += ("Apache-2.0", url("http://www.opensource.org/licenses/apache2.0")),
  resolvers += "Custom Spark build" at "http://ypg-data.github.io/repo",
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding", "UTF-8",
    "-feature",
    "-language:existentials",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-language:experimental.macros",
    "-unchecked",
    "-Xexperimental",
    // "-Xfatal-warnings", Off due to deprecation warnings from macro paradise
    "-Xlint",
    "-Xfuture",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-value-discard"
    // "-Ywarn-numeric-widen" Requires Scala 2.11: https://issues.scala-lang.org/browse/SI-8340
  ),
  // PermGen increased due to failing tests.
  // NOTE: This setting disappears with JDK8 however a warning message
  //        that the setting is being ignored pops up if the parameter is set
  //        see -> http://www.infoq.com/articles/Java-PERMGEN-Removed
  javaOptions += "-XX:MaxPermSize=256m",
  fork in Test := true
)

// Scala style guide: https://github.com/daniel-trinh/scalariform#scala-style-guide
ScalariformKeys.preferences := ScalariformKeys.preferences.value
   .setPreference(DoubleIndentClassDeclaration, true)
   .setPreference(PlaceScaladocAsterisksBeneathSecondAsterisk, true)

defaultScalariformSettings

lazy val scalaTest = Seq(
  "junit"            % "junit"        % "4.10"   % "test",
  "org.mockito"      % "mockito-core" % "1.9.0"  % "test",
  "org.scalatest"   %% "scalatest"    % "2.2.4"  % "test",
  "org.scalacheck"  %% "scalacheck"   % "1.12.1" % "test"
)

def sparkLibs(scalaVersion: String) = {
  val sparkVersion = CrossVersion.partialVersion(scalaVersion) match {
    case Some((2, 10)) => "1.3.1-DBC"
    case _ /* 2.11+ */ => "1.3.1"
  }

  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided"
  )
}

lazy val core = project
  .settings(
    name := "sparrow",
    commonSettings,
    defaultScalariformSettings,
    libraryDependencies ++= scalaTest ++ sparkLibs(scalaVersion.value) ++ Seq(
      "com.typesafe.play"      %% "play-functional" % "2.4.0-RC1",
      "org.scalaz"             %% "scalaz-core"     % "7.1.1", // https://github.com/scalaz/scalaz
      "com.github.nscala-time" %% "nscala-time"     % "1.8.0",
      "org.log4s"              %% "log4s"           % "1.1.5",
      "org.scala-lang"          % "scala-reflect"   % scalaVersion.value,
      compilerPlugin("org.scalamacros" % "paradise" % "2.0.1" cross CrossVersion.full)
    )
  )
