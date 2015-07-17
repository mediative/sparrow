
object Jvm {
  import sbt.Keys._
  import sbt._

  sealed trait Version { def v: String }
  case object V7 extends Version { def v = "1.7" }
  case object V8 extends Version { def v = "1.8" }

  def required(jvm: Version) = {
    Seq(
      javacOptions ++= Seq("-source", jvm.v, "-target", jvm.v, "-Xlint"),
      initialize := {
        val _ = initialize.value
        if (sys.props("java.specification.version") != jvm.v)
          sys.error(s"Java ${jvm.v} is required for this project.")
      }
    )
  }
}