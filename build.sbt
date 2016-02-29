organization in ThisBuild       := "com.mediative"
name in ThisBuild               := "sparrow"
scalaVersion in ThisBuild       := "2.10.5"
crossScalaVersions in ThisBuild := Seq("2.10.5", "2.11.7")

lazy val MacroParadiseVersion = "2.1.0"

lazy val sparkPackagesSettings = Seq(
  spName := "ypg-data/sparrow",
  sparkVersion := "1.6.0",
  sparkComponents += "sql",
  spAppendScalaVersion := true,
  credentials += Credentials(Path.userHome / ".credentials" / "spark-packages.properties")
)

lazy val scalaTest = Seq(
  "junit"            % "junit"        % "4.10"   % "test",
  "org.mockito"      % "mockito-core" % "1.9.0"  % "test",
  "org.scalatest"   %% "scalatest"    % "2.2.4"  % "test",
  "org.scalacheck"  %% "scalacheck"   % "1.12.1" % "test"
)

lazy val root = (project in file("."))
  .enablePlugins(MediativeReleasePlugin)
  .settings(
    name := "sparrow-project",
    noPublishSettings
  )
  .aggregate(core)

lazy val core = project
  .enablePlugins(MediativeGitHubPlugin, MediativeBintrayPlugin)
  .settings(
    name := "sparrow",
    // Off due to deprecation warnings from macro paradise
    scalacOptions := scalacOptions.value.filterNot { _ == "-Xfatal-warnings" },
    sparkPackagesSettings,
    libraryDependencies ++= scalaTest ++ Seq(
      "org.apache.spark"       %% "spark-core"      % sparkVersion.value % "provided",
      "org.apache.spark"       %% "spark-sql"       % sparkVersion.value % "provided",
      "com.typesafe.play"      %% "play-functional" % "2.4.0-RC1",
      "org.scalaz"             %% "scalaz-core"     % "7.1.1", // https://github.com/scalaz/scalaz
      "com.github.nscala-time" %% "nscala-time"     % "1.8.0",
      "org.log4s"              %% "log4s"           % "1.1.5",
      "org.scala-lang"          % "scala-reflect"   % scalaVersion.value,
      compilerPlugin("org.scalamacros" % "paradise" % MacroParadiseVersion cross CrossVersion.full)
    ) ++ {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, 10)) => Seq(
          "org.scalamacros" %% "quasiquotes" % MacroParadiseVersion
        )
        case _ /* 2.11+ */ => Seq.empty
      }
    }


  )
