package com.ua

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hive.hcatalog.api.HCatClient

import scala.collection.JavaConverters._

class HCatalogClient(file: String) {

  val hadoopConfig = new Configuration()
  hadoopConfig.addResource(new Path(file))
  val hCatClient = HCatClient.create(hadoopConfig)

  def getMaxBatchId(dbName: String, tableName: String, colName: String): Long = {

    val partitionValues = getPartitionValues(dbName, tableName)
    val columns = getPartitionColumns(dbName, tableName)

    val columnsData = partitionValues
      .flatMap(value => value.zip(columns))
      .filter { case (value, columnName) => columnName == colName }
      .map { case (value, columnName) => value.toLong }

    println("List size: " + columnsData.length)
    columnsData.max
  }


  private def getPartitionColumns(dbName: String, tableName: String): List[String] = {
    val partCols = hCatClient.getTable(dbName, tableName).getPartCols.asScala
    partCols.map(col => col.getName).toList
  }

  private def getPartitionValues(dbName: String, tableName: String): List[List[String]] = {
    val partitions = hCatClient.getPartitions(dbName, tableName).asScala
    partitions.map(_.getValues.asScala.toList).toList
  }

}
