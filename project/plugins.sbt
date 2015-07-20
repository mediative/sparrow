//resolvers += "typesafe maven" at "http://repo.typesafe.com/typesafe/maven-releases"

resolvers += "jgit-repo" at "http://download.eclipse.org/jgit/maven"

addSbtPlugin("com.eed3si9n"      % "sbt-buildinfo"    % "0.4.0")
addSbtPlugin("com.typesafe.sbt"  % "sbt-scalariform"  % "1.3.0")
addSbtPlugin("com.typesafe.sbt"  % "sbt-git"          % "0.8.4")
addSbtPlugin("com.typesafe.sbt"  % "sbt-ghpages"      % "0.5.3")
addSbtPlugin("me.lessis"         % "bintray-sbt"      % "0.3.0")
addSbtPlugin("de.heikoseeberger" % "sbt-header"       % "1.5.0")
