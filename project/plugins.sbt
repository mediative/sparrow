resolvers += Resolver.url("YPG-Data SBT Plugins", url("https://dl.bintray.com/ypg-data/sbt-plugins"))(Resolver.ivyStylePatterns)
resolvers += "Spark Package Main Repo" at "https://dl.bintray.com/spark-packages/maven"

addSbtPlugin("org.spark-packages" % "sbt-spark-package"  % "0.2.2")
addSbtPlugin("com.mediative.sbt"  % "sbt-mediative-core" % "0.1")
addSbtPlugin("com.mediative.sbt"  % "sbt-mediative-oss"  % "0.1")
