package com.ua


import com.typesafe.config.{Config, ConfigFactory}

object Main extends App {

  val myConf: Config = ConfigFactory.load()
  val file = myConf.getString("hive.path")
  val dbName = myConf.getString("hive.database")
  val tableName = myConf.getString("hive.table")
  val columnName = myConf.getString("hive.column")

  val myClient = new HCatalogClient(file)
  val maxBatchId = myClient.getMaxBatchId(dbName, tableName, columnName)
  println(s"Max batch id: $maxBatchId")
}
