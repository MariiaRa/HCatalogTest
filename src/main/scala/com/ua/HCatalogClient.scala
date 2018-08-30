package com.ua

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hive.hcatalog.api.HCatClient

import scala.collection.JavaConverters._

class HCatalogClient(file: String) {

  val hadoopConfig = new Configuration()
  hadoopConfig.addResource(new Path(file))
  val hCatClient = HCatClient.create(hadoopConfig)

  /**
    * Retrieves max batchId
    *
    * @param dbName    - name of database
    * @param tableName - name of table
    * @param colName   - column name (batchId)
    * @return          - max value of batchId
    */
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

  /**
    * Fetches list of names of partition columns in hive table
    *
    * @param dbName    - name of database
    * @param tableName - name of table
    * @return          - list of names of partition columns
    */
  private def getPartitionColumns(dbName: String, tableName: String): List[String] = {
    val partColumns = hCatClient.getTable(dbName, tableName).getPartCols.asScala
    partColumns.map(col => col.getName).toList
  }

  /**
    * Fetches list of all partitions in hive table
    *
    * @param dbName    - name of database
    * @param tableName - name of table
    * @return          - list of all partitions in table
    */
  private def getPartitionValues(dbName: String, tableName: String): List[List[String]] = {
    val partitions = hCatClient.getPartitions(dbName, tableName).asScala
    partitions.map(_.getValues.asScala.toList).toList
  }

}
