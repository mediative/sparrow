import sbt.Keys._

// import scalariform.formatter.preferences._

// addCommandAlias("format-code",    ";compile:scalariformFormat;test:scalariformFormat;it:scalariformFormat")

name := "sparrow"

lazy val buildSettings = Seq(
  organization       := "com.mediative",
  scalaVersion       := "2.10.4",
  crossScalaVersions := Seq("2.10.4", "2.10.5", "2.11.7")
)

enablePlugins(GitVersioning)
git.useGitDescribe := true

lazy val commonSettings = Seq(
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

// Macro-paradise is required with Scala 2.11 because of annotation macros
lazy val paradiseVersion = "2.0.1"
lazy val quasiquotes = libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
   compilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full)
)

// Scala style guide: https://github.com/daniel-trinh/scalariform#scala-style-guide
// ScalariformKeys.preferences := ScalariformKeys.preferences.value
//   .setPreference(DoubleIndentClassDeclaration, true)
//   .setPreference(PlaceScaladocAsterisksBeneathSecondAsterisk, true)

lazy val mpnBrainSettings =
  buildSettings ++
  commonSettings ++
  // defaultScalariformSettingsWithIt ++
  Jvm.required(Jvm.V7)

lazy val scalaTest = Seq(
  "junit"            % "junit"        % "4.10"   % "test",
  "org.mockito"      % "mockito-core" % "1.9.0"  % "test",
  "org.scalatest"   %% "scalatest"    % "2.2.4"  % "test",
  "org.scalacheck"  %% "scalacheck"   % "1.12.1" % "test"
)

lazy val logger = Seq(
  "org.log4s"  %% "log4s"         % "1.1.5"
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

lazy val sparkSettings = Seq(
  resolvers += "Custom Spark build" at "http://ypg-data.github.io/repo",
  parallelExecution in IntegrationTest := false,
  libraryDependencies ++= sparkLibs(scalaVersion.value) ++ Seq(
    "com.databricks" %% "spark-csv"  % "1.0.3"
  )
)

lazy val core = project
  .configs(IntegrationTest)
  .settings(
    mpnBrainSettings,
    quasiquotes,
    sparkSettings,
    libraryDependencies ++= scalaTest ++ logger ++ Seq(
      "com.typesafe.play"      %% "play-functional" % "2.4.0-RC1",
      "org.scalaz"             %% "scalaz-core"     % "7.1.1", // https://github.com/scalaz/scalaz
      "com.github.nscala-time" %% "nscala-time"     % "1.8.0"
    )
  )
