name := "WebLogChallenge"

version := "1.0"

scalaVersion := "2.11.8"

val overrideScalaVersion = "2.11.8"
val sparkVersion = "2.1.0"

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }
resolvers += "All Spark Repository -> bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion exclude("jline", "2.12"),
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)
