name := "tetraitesapi"

version := "0.0.4"

scalaVersion := "2.11.8"

resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"

resolvers += "bintray-tverbeiren" at "http://dl.bintray.com/tverbeiren/maven"

libraryDependencies ++= Seq(
  "spark.jobserver"    %% "job-server-api"    % "0.7.0a"     % "provided",
  "spark.jobserver"    %% "job-server-extras" % "0.7.0a"     % "provided",
  "org.apache.spark"   %% "spark-core"        % "2.0.1"      % "provided",
  "org.apache.spark"   %% "spark-mllib"       % "2.0.1"      % "provided"
)

organization := "info.verbeiren"
licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))
bintrayPackageLabels := Seq("tetraites")

// Publish assembly jar as well
artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.copy(`classifier` = Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)
