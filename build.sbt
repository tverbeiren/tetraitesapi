name := "tetraitesapi"

version := "0.0.1"

scalaVersion := "2.11.8"

resolvers += "Job Server Bintray" at
"https://dl.bintray.com/spark-jobserver/maven"

resolvers += "bintray-tverbeiren" at "http://dl.bintray.com/tverbeiren/maven"

libraryDependencies ++= Seq(
  "spark.jobserver"    %% "job-server-api"    % "0.7.0-SNAPSHOT"     % "provided",
  "spark.jobserver"    %% "job-server-extras" % "0.7.0-SNAPSHOT"     % "provided",
  "org.scalactic"      %% "scalactic"         % "3.0.0"     % "test"    ,
  "org.scalatest"      %% "scalatest"         % "3.0.0"     % "test"    ,
  "org.apache.spark"   %% "spark-core"        % "2.0.1"     % "provided"
)

