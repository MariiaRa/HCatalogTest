package com.ua

import java.text.SimpleDateFormat
import java.time.{Instant, LocalDate, ZoneId}
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
    val partitionValues = getPartitionValues(tableName, None)
    val columnNames = getPartitionColumns(tableName)

    val columnsData = partitionValues
      .flatMap(value => value.zip(columnNames))
      .filter { case (value, columnName) => columnName == defaultPartitionName }
      .map { case (value, columnName) => value.toLong }

    columnsData.max
  }

  /**
    * Retrieves max batchId from selected partition
    *
    * @param tableName   - name of table
    * @param filterKey   - name of partition column
    * @param filterValue - value of partition column
    * @return - max batchId from filtered partition
    */
  def getMaxBatchId(tableName: String, filterKey: String, filterValue: String): Long = {
    val partitionValues = getPartitionValues(tableName, Some(filterKey, filterValue))
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
  def getBatchIdRange(fromBatchId: Long, tableName: String, filter: PartitionFilter = None): List[Long] = {
    val partitionValues = getPartitionValues(tableName, None)
    val xs = partitionValues.dropWhile(list => !(list.last.toLong == fromBatchId))
    val columnNames = getPartitionColumns(tableName)

    filter match {
      case Some((name: String, value: String)) => {
        val columnNames = getPartitionColumns(tableName)
        xs
          .map(value => value.zip(columnNames)).filter(listOfTuples => listOfTuples.contains((value, name)))
          .sortWith(_.last._1.toLong < _.last._1.toLong)
          .map(_.last._1.toLong)
      }
      case None => xs.sortWith(_.last.toLong < _.last.toLong).map(_.last.toLong)
    }
  }

  /**
    * Retrieves max batchId from selected partition column
    *
    * @param tableName     - name of table
    * @param partitionName - name of partition column
    * @return - max batchId in selected partition column
    */
  def getMaxPartitionId(tableName: String, partitionName: String): Long = getMaxBatchId(tableName) //another name for batchId?

  /**
    * Retrieves max batchId from filtered partition
    *
    * @param tableName     - name of table
    * @param partitionName - name of partition column
    * @param filterKey     - partition key
    * @param filterValue   - partition value
    * @return - max batchId from filtered partition
    */
  def getMaxPartitionId(tableName: String, partitionName: String, filterKey: String, filterValue: String): Long = getMaxBatchId(tableName, filterKey, filterValue)

  /**
    * Retrieves batchId range from filtered partition sorted in ascending order
    *
    * @param fromBatchId   - value of batchId
    * @param tableName     - name of table
    * @param partitionName - name of partition column
    * @param filter        - partition key & partition value
    * @return - batchId range sorted in ascending order
    */
  def getMaxPartitionIdRange(fromBatchId: Long, tableName: String, partitionName: String, filter: PartitionFilter = None): List[Long] =
    getBatchIdRange(fromBatchId, tableName, filter)

  /**
    * Retrieves max date from partition column
    *
    * @param tableName     - name of table
    * @param partitionName - name of partition column with date
    * @return - max value of date in partition column
    */
  def getMaxDate(tableName: String, partitionName: String): Option[LocalDate] = {
    val partitionValues = getPartitionValues(tableName, None)
    val columnNames = getPartitionColumns(tableName)
    val format = new SimpleDateFormat(dateFormat)

    val columnsData = partitionValues
      .flatMap(value => value.zip(columnNames))
      .filter { case (value, columnName) => columnName == partitionName }
      .map { case (value, columnName) => format.parse(value) }

    if (columnsData.nonEmpty) {
      Some(
        Instant.ofEpochMilli(
          columnsData.max.getTime
        ).atZone(ZoneId.systemDefault()).toLocalDate)
    }
    else {
      None
    }
  }

  /**
    * Retrieves map of dates and max batchId values from partition column starting from specified batchId
    *
    * @param tableName   - name of table
    * @param fromBatchId - value of batchId
    * @param filterKey   - partition key
    * @param filterValue - partition value
    * @return - map of dates and max batchId values for given date starting from specified batchId
    */
  def getDateRange(tableName: String, fromBatchId: Long, filterKey: String, filterValue: String): Map[Date, Long] = {
    val format = new SimpleDateFormat(dateFormat)
    val partitionValues = getPartitionValues(tableName, None)
    val xs = partitionValues.dropWhile(list => !(list.last.toLong == fromBatchId))
    val columnNames = getPartitionColumns(tableName)

    xs
      .map(value => value.zip(columnNames)).filter(listOfTuples => listOfTuples.contains((filterValue, filterKey)))
      .sortWith(_.last._1.toLong < _.last._1.toLong)
      .map(list => format.parse(list.head._1) -> list.last._1.toLong).toMap
  }

  /**
    * Retrieves map of dates and max batchId values starting from specified batchId
    *
    * @param tableName   - name of table
    * @param fromBatchId - value of batchId
    * @return - map of dates and max batchId values starting from specified batchId
    */
  def getDateRange(tableName: String, fromBatchId: Long): Map[Date, Long] = {
    val partitionValues: List[List[String]] = getPartitionValues(tableName, None)
    val format = new SimpleDateFormat(dateFormat)
    val xs = partitionValues.dropWhile(list => !(list.last.toLong == fromBatchId))

    xs.sortWith(_.last.toLong < _.last.toLong).map(list => format.parse(list.head) -> list.last.toLong).toMap
  }

  /**
    * Fetches list of names of partition columns in hive table
    *
    * @param tableName - name of table
    * @return - list of names of partition columns
    */
  private def getPartitionColumns(tableName: String): List[String] = {
    val partColumns = hCatClient.getTable(databaseName, tableName).getPartCols.asScala
    partColumns.map(_.getName).toList
  }

  /**
    * Fetches list of all partitions in hive table
    *
    * @param tableName       - name of table
    * @param partitionFilter - partition key & partition value
    * @return - list of all partition values in table
    */
  private def getPartitionValues(tableName: String, partitionFilter: Option[(String, String)]): List[List[String]] = {
    partitionFilter match {
      case Some((name: String, value: String)) => {
        val partitions = hCatClient.getPartitions(databaseName, tableName, Map[String, String](name -> value).asJava).asScala
        partitions.map(_.getValues.asScala.toList).toList
      }
      case _ => {
        val partitions = hCatClient.getPartitions(databaseName, tableName).asScala
        partitions.map(_.getValues.asScala.toList).toList
      }
    }
  }
}

object HiveMetastoreClient {
  type FilterKey = String
  type FilterValue = String
  type PartitionFilter = Option[(FilterKey, FilterValue)]
  val defaultPartitionName: String = "rddid" //"batch_id"
  val dateFormat = "yyyy-MM-dd"

  def apply(
      metastoreConnectionURL: String,
      metastoreConnectionDriverName: String,
      metastoreUserName: String,
      metastorePassword: String,
      metastoreUris: String,
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
