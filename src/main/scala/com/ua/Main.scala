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

  val maxBatchIdwithFilter = myClient.getMaxBatchId(tableName, "dateid", "2018-08-31-17-57")
  println(s"max batchId with filter: $maxBatchIdwithFilter")

  println(s"max batchId in partition: ${myClient.getMaxPartitionId(tableName, "dateid")}")

  println(s"max batchId in partition with filter: ${myClient.getMaxPartitionId(tableName, "dateid", "dateid", "2018-08-31-17-57")}")

  println(s"range of max batchIds with filter: ${myClient.getMaxPartitionIdRange(1535727470003L, tableName, "dateid", Some("dateid", "2018-08-31-17-57"))}")

  val maxDate = myClient.getMaxDate(tableName, "dateid")
  println(s"Max date id: $maxDate")

  val maxBatchIdRange = myClient.getBatchIdRange(1535727470003L, tableName)
  println(s"Batchid range: $maxBatchIdRange")

  val dateRange = myClient.getDateRange(tableName, 1535727470003L)
  println(s"Date map: $dateRange")

  val dateRangeTwo = myClient.getDateRange(tableName, 1535727470003L, "dateid", "2018-08-31-17-57")
  println(s"Date map with filter: $dateRangeTwo")
}
