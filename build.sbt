name := "HCatalogTest"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.2",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "org.apache.hive.hcatalog" % "hive-webhcat-java-client" % "3.1.0" exclude("javax.jms", "jms")
)