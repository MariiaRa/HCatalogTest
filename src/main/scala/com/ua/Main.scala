package com.ua


import com.typesafe.config.{Config, ConfigFactory}

object Main extends App {

  val myConf: Config = ConfigFactory.load()
  val file = myConf.getString("hive.path")
  val dbName = myConf.getString("hive.database")
  val tableName = myConf.getString("hive.table")

  val myClient = new HCatalogClient
  val parts = myClient.getPartitionValues(dbName, tableName, file)
  println(parts.max)


}
