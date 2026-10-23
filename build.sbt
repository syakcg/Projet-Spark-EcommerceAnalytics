name := "EcommerceAnalytics"
version := "1.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.4.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
  "com.typesafe"     %  "config"     % "1.4.2",
  "org.scalatest"    %% "scalatest"  % "3.2.15" % Test
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}