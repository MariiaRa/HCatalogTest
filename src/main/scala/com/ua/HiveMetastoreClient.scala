package com.ua

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hive.hcatalog.api.HCatClient

import scala.collection.JavaConverters._


class HiveMetastoreClient(hiveConfFile: String) {

  private val defaultPartitionName: String = "rddid" //"batch_id"

  val hadoopConfig = new Configuration()
  hadoopConfig.addResource(new Path(hiveConfFile))
  val hCatClient: HCatClient = HCatClient.create(hadoopConfig)

  /**
    * Retrieves max batchId
    *
    * @param tableName - name of table
    * @return - max value of batchId
    */
  def getMaxBatchId(databaseName: String, tableName: String): Long = {
    val partitionValues = getPartitionValues(databaseName, tableName)
    val columns = getPartitionColumns(databaseName, tableName)

    val columnsData = partitionValues
      .flatMap(value => value.zip(columns))
      .filter { case (value, columnName) => columnName == defaultPartitionName }
      .map { case (value, columnName) => value.toLong }

    columnsData.max
  }

  /**
    * Retrieves list of batchId starting from given value
    *
    * @param tableName   - name of table
    * @param fromBatchId - value of batchId
    * @return - batchId range sorted in ascending order
    */
  def getMaxBatchIdRange(databaseName: String, tableName: String, fromBatchId: Long): List[Long] = {
    val partitionValues = getPartitionValues(databaseName, tableName)
    val columns = getPartitionColumns(databaseName, tableName)

    val columnsData = partitionValues
      .flatMap(value => value.zip(columns))
      .filter { case (value, columnName) => columnName == defaultPartitionName }
      .map { case (value, columnName) => value.toLong }
      .dropWhile(value => !(value == fromBatchId))

    columnsData.sortWith(_ < _)
  }

  /**
    * Retrieves max date
    *
    * @param databaseName  - name of database
    * @param tableName     - name of table
    * @param partitionName - name of partition column with date
    * @return - max value of date in partition column
    */
  def getMaxDate(databaseName: String, tableName: String, partitionName: String): Date = {

    val partitionValues = getPartitionValues(databaseName, tableName)
    val columnNames = getPartitionColumns(databaseName, tableName)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm")

    val columnsData = partitionValues
      .flatMap(value => value.zip(columnNames))
      .filter { case (value, columnName) => columnName == partitionName }
      .map { case (value, columnName) => dateFormat.parse(value) }

    columnsData.max
  }

  /**
    * Retrieves map of dates and max batchId values for given date starting from specified batchId
    *
    * @param databaseName - name of database
    * @param tableName    - name of table
    * @param fromBatchId  - value of batchId
    * @return - map of dates and max batchId values for given date starting from specified batchId
    */
  def getDateRange(databaseName: String, tableName: String, fromBatchId: Long) = {

    val partitionValues: List[List[String]] = getPartitionValues(databaseName, tableName)

    val convertedList = convertColumnValue(databaseName: String, tableName: String, partitionValues)
      .sortWith(_.last.asInstanceOf[Long] < _.last.asInstanceOf[Long])
      .dropWhile(list => !list.contains(fromBatchId)).map(list => list.head -> list.last).toMap

    convertedList
  }

  /**
    * Fetches list of names of partition columns in hive table
    *
    * @param databaseName - name of database
    * @param tableName    - name of table
    * @return - list of names of partition columns
    */
  private def getPartitionColumns(databaseName: String, tableName: String): List[String] = {
    val partColumns = hCatClient.getTable(databaseName, tableName).getPartCols.asScala
    partColumns.map(col => col.getName).toList
  }

  /**
    * Fetches list of all partitions in hive table
    *
    * @param databaseName - name of database
    * @param tableName    - name of table
    * @return - list of all partitions in table
    */
  private def getPartitionValues(databaseName: String, tableName: String): List[List[String]] = {
    val partitions = hCatClient.getPartitions(databaseName, tableName).asScala.toList
    partitions.map(_.getValues.asScala.toList)
  }

  private def getColumnTypes(databaseName: String, tableName: String): Map[String, String] = {
    val partColumns = hCatClient.getTable(databaseName, tableName).getPartCols.asScala
    partColumns.map(column => column.getName -> column.getTypeInfo.getTypeName).toMap
  }

  private def convertColumnValue(databaseName: String, tableName: String, list: List[List[String]]) = {
    val mapOfTypes = getColumnTypes(databaseName, tableName)
    val columnNames = getPartitionColumns(databaseName, tableName)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm")

    list.map(value => value.zip(columnNames)).map { list =>
      list.map { elem =>
        mapOfTypes(elem._2) match {
          case "string" => dateFormat.parse(elem._1) //should be date in hive table
          case "bigint" => elem._1.toLong
        }
      }
    }
  }
}