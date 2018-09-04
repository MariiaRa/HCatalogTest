package com.ua


import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object Main extends App {

  val myConf: Config = ConfigFactory.load()
  val file = myConf.getString("hive.path")
  val dbName = myConf.getString("hive.database")
  val tableName = myConf.getString("hive.table")
  val columnName = myConf.getString("hive.column")
  val dateFormat = "yyyy-MM-dd"
  val hadoopConfig = new Configuration()
  hadoopConfig.addResource(new Path(file))

  val myClient = new HiveMetastoreClient(hadoopConfig)

  val maxBatchId = myClient.getMaxBatchId(dbName, tableName)
  println(s"max batchId: $maxBatchId")

  val maxDate = myClient.getMaxDate(dbName, tableName, "dateid", dateFormat)
  println(s"Max date id: $maxDate")

  val maxBatchIdrane = myClient.getMaxBatchIdRange(dbName, tableName, 1535641220002L)

  val dateRange = myClient.getDateRange(dbName, tableName, 1535641220002L, dateFormat)
  println(s"Range: $dateRange")
}
