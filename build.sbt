name := "HCatalogTest"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.hive.hcatalog" % "hive-webhcat-java-client" % "2.3.2"
    exclude("javax.jms", "jms")
    exclude("org.pentaho", "pentaho-aggdesigner-algorithm"),
  "com.typesafe" % "config" % "1.3.2",
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "org.apache.logging.log4j" % "log4j-api" % "2.8",
  "org.apache.logging.log4j" % "log4j-core" % "2.8"
)