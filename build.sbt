name := "HCatalogTest"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.hive.hcatalog" % "hive-webhcat-java-client" % "1.1.0" exclude("javax.jms", "jms") exclude("eigenbase", "eigenbase-properties")
    exclude("org.pentaho", "pentaho-aggdesigner-algorithm"),
  "org.apache.logging.log4j" % "log4j-api" % "2.11.1",
  "org.apache.hadoop" % "hadoop-common" % "2.6.0",
  "com.typesafe" % "config" % "1.3.2"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}