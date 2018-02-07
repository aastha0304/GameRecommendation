name := "game_recommendation"

version := "1.0"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-deprecation")

resolvers += Resolver.sonatypeRepo("releases")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0" ,
  "org.apache.spark" %% "spark-sql" % "2.2.0",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "com.typesafe" % "config" % "1.3.1",
  "junit" % "junit" % "4.10" % Test
)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
fork := true