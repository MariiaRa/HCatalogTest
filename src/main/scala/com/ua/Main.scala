package com.ua


import com.typesafe.config.{Config, ConfigFactory}

object Main extends App {

  val myConf: Config = ConfigFactory.load()
  val file = myConf.getString("hive.path")
  val dbName = myConf.getString("hive.database")
  val tableName = myConf.getString("hive.table")
  val columnName = myConf.getString("hive.column")

  val myClient = new HiveMetastoreClient(file)

  val maxBatchId = myClient.getMaxBatchId(dbName, tableName)
  println(s"max batchId: $maxBatchId")

  val maxDate = myClient.getMaxDate(dbName, tableName, "dateid")
  println(s"Max date id: $maxDate")

  val maxBatchIdrane = myClient.getMaxBatchIdRange(dbName, tableName, 1535727460003L)
  println(s"Range: $maxBatchIdrane")

  val dateRange = myClient.getDateRange(dbName, tableName, 1535727460003L)
  println(s"Range: $dateRange")
}
