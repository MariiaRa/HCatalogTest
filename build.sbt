name := "HCatalogTest"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.hive.hcatalog" % "hive-webhcat-java-client" % "2.3.2" exclude("javax.jms", "jms") exclude("eigenbase", "eigenbase-properties")
    exclude("org.pentaho", "pentaho-aggdesigner-algorithm"),
  "com.typesafe" % "config" % "1.3.2"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}