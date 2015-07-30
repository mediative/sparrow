import sbt.Keys._
import de.heikoseeberger.sbtheader.license.Apache2_0
import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtSite.SiteKeys.siteMappings
import com.typesafe.sbt.SbtGhPages.GhPagesKeys.{ pushSite => ghpagesPushSite }
import ReleaseTransformations._
import ReleaseKeys._

addCommandAlias("format", ";compile:scalariformFormat;test:scalariformFormat")
addCommandAlias("update-license", ";compile:createHeaders;test:createHeaders")

enablePlugins(GitVersioning)
git.useGitDescribe := true

lazy val commonSettings = Seq(
  organization       := "com.mediative",
  scalaVersion       := "2.10.5",
  crossScalaVersions := Seq("2.10.5", "2.11.7"),
  licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
  headers := Map("scala" -> Apache2_0("2015", "Mediative")),
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

lazy val noPublishSettings = Seq(
  publish := (),
  publishLocal := (),
  publishArtifact := false
)

lazy val publishSettings = Seq(
  homepage := Some(url("https://github.com/ypg-data/sparrow")),
  apiURL := Some(url("https://ypg-data.github.io/sparrow/api/")),
  autoAPIMappings := true,
  publishArtifact in Test := false,
  publishMavenStyle := false,
  bintrayRepository := "ivy-releases",
  bintrayOrganization := Some("ypg-data")
)

lazy val sparkPackagesSettings = Seq(
  spName := "ypg-data/sparrow",
  sparkVersion := "1.3.1",
  sparkComponents += "sql",
  spAppendScalaVersion := true,
  spIncludeMaven := true,
  credentials += Credentials(Path.userHome / ".credentials" / "spark-packages.properties")
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

/*
 * Customized release process that reads the release version from the
 * command line via `-Dversion=x.y.z` and extends the publishing step to
 * also update of API docs hosted on the project's GitHub pages.
 *
 * Compared to the default sbt-release process, all steps that update
 * and commit the version file before and after tagging the release have
 * been removed, since for this project SBT is configured to read the
 * project version from Git.
 */
lazy val releaseSettings = Seq(
  releaseCrossBuild := true,
  releaseTagComment := s"Releasing ${(version in ThisBuild).value}",
  releaseTagName := (version in ThisBuild).value,
  releaseVersionFile := target.value / "unused-version.sbt",
  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    { st: State =>
      val v = sys.props.getOrElse("version", sys.error("No version specified"))
      st.put(versions, (v, v))
    },
    runTest,
    setReleaseVersion,
    tagRelease,
    publishArtifacts,
    releaseStepTask(ghpagesPushSite in core),
    pushChanges
  )
)

lazy val root = (project in file("."))
  .settings(
    name := "sparrow-project",
    noPublishSettings,
    releaseSettings
  )
  .aggregate(core)

lazy val core = project
  .settings(
    name := "sparrow",
    commonSettings,
    publishSettings,
    sparkPackagesSettings,
    site.settings,
    ghpages.settings,
    site.includeScaladoc("api"),
    git.remoteRepo := "git@github.com:ypg-data/sparrow.git",
    defaultScalariformSettings,
    libraryDependencies ++= scalaTest ++ Seq(
      "org.apache.spark"       %% "spark-core"      % "1.3.1" % "provided",
      "org.apache.spark"       %% "spark-sql"       % "1.3.1" % "provided",
      "com.typesafe.play"      %% "play-functional" % "2.4.0-RC1",
      "org.scalaz"             %% "scalaz-core"     % "7.1.1", // https://github.com/scalaz/scalaz
      "com.github.nscala-time" %% "nscala-time"     % "1.8.0",
      "org.log4s"              %% "log4s"           % "1.1.5",
      "org.scala-lang"          % "scala-reflect"   % scalaVersion.value,
      compilerPlugin("org.scalamacros" % "paradise" % "2.0.1" cross CrossVersion.full)
    )
  )
