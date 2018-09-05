package com.ua


import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration

object Main extends App {

  val myConf: Config = ConfigFactory.load()
  val dbName = myConf.getString("hive.database")
  val tableName = myConf.getString("hive.table")
  val columnName = myConf.getString("hive.column")
  val dateFormat = "yyyy-MM-dd"
  val hadoopConfig = new Configuration()

  val myClient = HiveMetastoreClient.apply(
    "jdbc:postgresql://hive-metastore-postgresql/metastore",
    "org.postgresql.Driver",
    "hive",
    "hive",
    "thrift://hive-metastore:9083",
    "default"
  )

  val maxBatchId = myClient.getMaxBatchId(tableName)
  println(s"max batchId: $maxBatchId")

  val maxDate = myClient.getMaxDate(tableName, "dateid", dateFormat)
  println(s"Max date id: $maxDate")

  val maxBatchIdrane = myClient.getMaxBatchIdRange(tableName, 1535641220002L)

  val dateRange = myClient.getDateRange(tableName, 1535641220002L, dateFormat)
  println(s"Range: $dateRange")
}
