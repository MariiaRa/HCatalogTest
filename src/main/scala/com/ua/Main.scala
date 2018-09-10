package com.ua


import com.typesafe.config.{Config, ConfigFactory}

object Main extends App {
  val myConf: Config = ConfigFactory.load()
  val tableName = myConf.getString("hive.table")
  val dbName = myConf.getString("hive.database")

  val myClient = HiveMetastoreClient.apply(
    "jdbc:postgresql://hive-metastore-postgresql/metastore",
    "org.postgresql.Driver",
    "hive",
    "hive",
    "thrift://hive-metastore:9083",
    dbName
  )

  val maxBatchId = myClient.getMaxBatchId(tableName)
  println(s"Max batchId: $maxBatchId")
  println(s"Max batchId in table with one partition: ${myClient.getMaxBatchId("data_one")}")

  val maxBatchIdwithFilter = myClient.getMaxBatchId(tableName, "dateid", "2018-08-31-17-57")
  println(s"Max batchId with filter: $maxBatchIdwithFilter")

  println(s"Max batchId in partition: ${myClient.getMaxPartitionId(tableName, "dateid")}")

  println(s"Max batchId in partition in table with one partition: ${myClient.getMaxPartitionId("data_one", "rddid")}")

  println(s"Max batchId in partition with filter: ${myClient.getMaxPartitionId(tableName, "dateid", "dateid", "2018-08-31-17-57")}")

  println(s"Range of max batchIds with filter: ${myClient.getMaxPartitionIdRange(1535727470003L, tableName, "dateid", Some("dateid", "2018-08-31-17-57"))}")

  val maxDate = myClient.getMaxDate(tableName, "dateid")
  println(s"Max date id: $maxDate")

  val maxBatchIdRange = myClient.getBatchIdRange(1535727470003L, tableName)
  println(s"Batchid range: $maxBatchIdRange")

  println(s"Batchid range in table with one partition: ${myClient.getBatchIdRange(1536324390002L, "data_one")}")

  val dateRange = myClient.getDateRange(tableName, 1535727470003L)
  println(s"Date map: $dateRange")

  val dateRangeTwo = myClient.getDateRange(tableName, 1535727470003L, "dateid", "2018-08-31-17-57")
  println(s"Date map with filter: $dateRangeTwo")

  /* val configsHive = new HiveConf
   configsHive.set("javax.jdo.option.ConnectionURL", "jdbc:postgresql://hive-metastore-postgresql/metastore")
   configsHive.set("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver")
   configsHive.set("javax.jdo.option.ConnectionPassword", "hive")
   configsHive.set("javax.jdo.option.ConnectionUserName", "hive")
   configsHive.set("hive.metastore.uris", "thrift://hive-metastore:9083")
   val metastoreClient = new HiveMetaStoreClient(configsHive)

   println("All tables from db: " + metastoreClient.getAllTables("default"))
   println("All partition values: " + metastoreClient.listPartitions("default", tableName, -1).asScala.toList.map(_.getValues))*/

}
