name := "HCatalogTest"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.hive.hcatalog" % "hive-webhcat-java-client" % "2.3.2" exclude("javax.jms", "jms") exclude("org.pentaho", "pentaho-aggdesigner-algorithm"),
  "com.typesafe" % "config" % "1.3.2",
  "org.mockito" % "mockito-core" % "1.10.19" % Test,
  "org.scalatest" %% "scalatest" % "3.0.5" % Test
)