package com.ua

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hive.hcatalog.api._

import scala.collection.JavaConverters._


class HiveMetastoreClient(hCatClient: HCatClient, databaseName: String) {

  import HiveMetastoreClient._
  /**
    * Retrieves max batchId
    *
    * @param tableName - name of table
    * @return - max value of batchId
    */
  def getMaxBatchId(tableName: String): Long = {
    val partitionValues = getPartitionValues(tableName)
    val columnNames = getPartitionColumns(tableName)

    val columnsData = partitionValues
      .flatMap(value => value.zip(columnNames))
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
  def getMaxBatchIdRange(tableName: String, fromBatchId: Long): List[Long] = {
    val partitionValues = getPartitionValues(tableName)
    val columnNames = getPartitionColumns(tableName)

    val columnsData = partitionValues
      .flatMap(value => value.zip(columnNames))
      .filter { case (value, columnName) => columnName == defaultPartitionName }
      .map { case (value, columnName) => value.toLong }
      .dropWhile(value => !(value == fromBatchId))

    columnsData.sortWith(_ < _)
  }

  /**
    * Retrieves max date
    *
    * @param tableName     - name of table
    * @param partitionName - name of partition column with date
    * @return - max value of date in partition column
    */
  def getMaxDate(tableName: String, partitionName: String, format: String): Date = {

    val partitionValues = getPartitionValues(tableName)
    val columnNames = getPartitionColumns(tableName)
    val dateFormat = new SimpleDateFormat(format)

    val columnsData = partitionValues
      .flatMap(value => value.zip(columnNames))
      .filter { case (value, columnName) => columnName == partitionName }
      .map { case (value, columnName) => dateFormat.parse(value) }

    columnsData.max
  }

  /**
    * Retrieves map of dates and max batchId values for given date starting from specified batchId
    *
    * @param tableName    - name of table
    * @param fromBatchId  - value of batchId
    * @return - map of dates and max batchId values for given date starting from specified batchId
    */
  def getDateRange(tableName: String, fromBatchId: Long, format: String) = {

    val partitionValues: List[List[String]] = getPartitionValues(tableName)

    val convertedList = convertColumnType(tableName: String, partitionValues, format)
      .sortWith(_.last.asInstanceOf[Long] < _.last.asInstanceOf[Long])
      .dropWhile(list => !list.contains(fromBatchId)).map(list => list.head -> list.last).toMap

    convertedList
  }

  /**
    * Fetches list of names of partition columns in hive table
    *
    * @param tableName    - name of table
    * @return - list of names of partition columns
    */
  private def getPartitionColumns(tableName: String): List[String] = {
    val partColumns = hCatClient.getTable(databaseName, tableName).getPartCols.asScala
    partColumns.map(_.getName).toList
  }

  /**
    * Fetches list of all partitions in hive table
    *
    * @param tableName    - name of table
    * @return - list of all partitions in table
    */
  private def getPartitionValues(tableName: String): List[List[String]] = {
    val partitions = hCatClient.getPartitions(databaseName, tableName).asScala
    partitions.map(_.getValues.asScala.toList).toList
  }

  /**
    * Creates map of column names and its types
    *
    * @param tableName    - name of table
    * @return - map of column names and its types
    */
  private def getPartColumnTypes(tableName: String): Map[String, String] = {
    val partColumns = hCatClient.getTable(databaseName, tableName).getPartCols.asScala
    partColumns.map(column => column.getName -> column.getTypeInfo.getTypeName).toMap
  }

  /**
    * Convert values to type according to types in hive table
    *
    * @param tableName    - name of table
    * @param list         - list of partition values
    * @param format       - date format (e.g., )
    * @return - list of partition values with converted types according to hive table
    */
  private def convertColumnType(tableName: String, list: List[List[String]], format: String) = {
    val mapOfTypes = getPartColumnTypes(tableName)
    val columnNames = getPartitionColumns(tableName)
    val dateFormat = new SimpleDateFormat(format)

    list.map(listOfValues => listOfValues.zip(columnNames)).map { listOfTuples =>
      listOfTuples.map { case (value, columnName) =>
        mapOfTypes(columnName) match {
          case "string" => dateFormat.parse(value)
          case "bigint" => value.toLong
          case _ => value
        }
      }
    }
  }
}

object HiveMetastoreClient {

  val defaultPartitionName: String = "rddid" //"batch_id"

  def apply(metastoreConnectionURL: String, metastoreConnectionDriverName: String,
            metastoreUserName: String, metastorePassword: String, metastoreUris: String,
            hiveDbName: String): HiveMetastoreClient = {

    val hadoopConfig = new Configuration()
    hadoopConfig.set("javax.jdo.option.ConnectionURL", metastoreConnectionURL)
    hadoopConfig.set("javax.jdo.option.ConnectionDriverName", metastoreConnectionDriverName)
    hadoopConfig.set("javax.jdo.option.ConnectionPassword", metastoreUserName)
    hadoopConfig.set("javax.jdo.option.ConnectionUserName", metastorePassword)
    hadoopConfig.set("hive.metastore.uris", metastoreUris)

    new HiveMetastoreClient(HCatClient.create(hadoopConfig), hiveDbName)
  }
}
