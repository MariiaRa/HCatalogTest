package com.ua


import java.text.SimpleDateFormat
import java.time.{Instant, LocalDate, ZoneId}
import java.util.Date

import com.ua.HiveMetastoreClient.dateFormat
import org.apache.hive.hcatalog.api.{HCatPartition, HCatTable}
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema.Type

import scala.collection.JavaConverters._

class TestHCatClient(databaseName: String, tableName: String) {

  import TestHCatClient._

  private def createTable(databaseName: String, tableName: String): HCatTable = {
    val table: HCatTable = new HCatTable(databaseName, tableName)
    val colOne = new HCatFieldSchema("id", Type.STRING, null)
    val colTwo = new HCatFieldSchema("value", Type.DECIMAL, null)
    val columns = List(colOne, colTwo).asJava
    table.cols(columns)
    val colPartOne = new HCatFieldSchema(datePartitionName, Type.STRING, null)
    val colPartTwo = new HCatFieldSchema(batchIdPartitionName, Type.STRING, null)
    val columnsPart = List(colPartOne, colPartTwo).asJava
    table.partCols(columnsPart)
  }


  private def getPartition(partitionFilter: Option[(String, String)]): java.util.List[HCatPartition] = {
    val table = createTable(databaseName, tableName)
    val partition1 = new HCatPartition(table, partitionKeyValues1, path)
    val partition2 = new HCatPartition(table, partitionKeyValues2, path)
    val partition3 = new HCatPartition(table, partitionKeyValues3, path)
    val partition4 = new HCatPartition(table, partitionKeyValues4, path)
    val partition5 = new HCatPartition(table, partitionKeyValues5, path)
    val partition6 = new HCatPartition(table, partitionKeyValues6, path)
    val partition7 = new HCatPartition(table, partitionKeyValues7, path)
    val partitions = List[HCatPartition](partition1, partition2, partition3, partition4, partition5, partition6, partition7).asJava

    partitionFilter match {
      case Some((name: String, value: String)) => {
        partitions.asScala.filter(_.getPartitionKeyValMap.asScala.exists(_ == name -> value)).asJava
      }
      case None => partitions
    }
  }

  private def getMaxPartitionValue(partitionName: String, partitionFilter: Option[(String, String)]): Long = {
    val partitionValues = getPartition(partitionFilter).asScala.map(_.getValues.asScala.toList).toList

    val columnsData = partitionValues
      .flatMap(value => value.zip(partColumnsNames))
      .filter { case (value, columnName) => columnName == partitionName }
      .map { case (value, columnName) => value.toLong }

    columnsData.max
  }

  private def sortByBatchId(inputOne: List[(String, String)], inputTwo: List[(String, String)]): Boolean = inputOne.last._1.toLong < inputTwo.last._1.toLong

  def getMaxBatchId(): Long = getMaxPartitionValue(batchIdPartitionName, None)

  def getMaxBatchId(filterKey: String, filterValue: String): Long = getMaxPartitionValue(batchIdPartitionName, Some(filterKey, filterValue))

  def getBatchIdRange(fromBatchId: Long, filter: PartitionFilter = None): List[Long] = getMaxPartitionIdRange(fromBatchId, batchIdPartitionName, filter)

  def getMaxPartitionId(partitionName: String, filterKey: String, filterValue: String): Long = getMaxPartitionValue(partitionName, Some(filterKey, filterValue))

  def getMaxPartitionId(partitionName: String): Long = getMaxPartitionValue(partitionName, None)

  def getMaxPartitionIdRange(fromBatchId: Long, partitionName: String, filter: PartitionFilter = None): List[Long] = {
    val partitionValues = getPartition(None).asScala.map(_.getValues.asScala.toList).toList
    val xs = partitionValues.dropWhile(list => !(list.last.toLong == fromBatchId))

    filter match {
      case Some((name: String, value: String)) => {
        xs
          .map(value => value.zip(partColumnsNames)).filter(listOfTuples => listOfTuples.contains((value, name)) & listOfTuples.last._2 == partitionName)
          .sortWith(sortByBatchId(_, _))
          .map(_.last._1.toLong)
      }
      case None => {
        xs
          .flatMap(value => value.zip(partColumnsNames))
          .filter { case (_, columnName) => columnName == partitionName }
          .map { case (value, _) => value.toLong }
          .sortWith(_ < _)
      }
    }
  }

  def getMaxDate(partitionName: String): Option[LocalDate] = {
    val partitionValues: List[List[String]] = getPartition(None).asScala.map(_.getValues.asScala.toList).toList
    val dateFormat = new SimpleDateFormat(format)

    val columnsData = partitionValues
      .flatMap(value => value.zip(partColumnsNames))
      .filter { case (value, columnName) => columnName == partitionName }
      .map { case (value, columnName) => dateFormat.parse(value) }

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

  def getDateRange(fromBatchId: Long, filterKey: String, filterValue: String): Map[Date, Long] = {
    val partitionValues = getPartition(None).asScala.map(_.getValues.asScala.toList).toList
    val format = new SimpleDateFormat(dateFormat)
    val xs = partitionValues.dropWhile(list => !(list.last.toLong == fromBatchId))

    xs
      .map(value => value.zip(partColumnsNames)).filter(listOfTuples => listOfTuples.contains((filterValue, filterKey)))
      .sortWith(sortByBatchId(_, _))
      .map(list => format.parse(list.head._1) -> list.last._1.toLong).toMap
  }


  def getDateRange(fromBatchId: Long): Map[Date, Long] = {
    val partitionValues: List[List[String]] = getPartition(None).asScala.map(_.getValues.asScala.toList).toList

    val format = new SimpleDateFormat(dateFormat)
    val xs = partitionValues.dropWhile(list => !(list.last.toLong == fromBatchId))

    xs.sortWith(_.last.toLong < _.last.toLong).map(list => format.parse(list.head) -> list.last.toLong).toMap
  }
}

object TestHCatClient {
  type FilterKey = String
  type FilterValue = String
  type PartitionFilter = Option[(FilterKey, FilterValue)]
  val format = "yyyy-MM-dd"
  val batchIdPartitionName: String = "batch_id"
  val datePartitionName: String = "date"
  val partColumnsNames = List(datePartitionName, batchIdPartitionName)
  val partitionKeyValues1 = Map[String, String](datePartitionName -> "2018-08-30-10-25", batchIdPartitionName -> "1535613940012").asJava
  val partitionKeyValues2 = Map[String, String](datePartitionName -> "2018-08-31-17-57", batchIdPartitionName -> "1535727475002").asJava
  val partitionKeyValues3 = Map[String, String](datePartitionName -> "2018-08-31-17-57", batchIdPartitionName -> "1535727445003").asJava
  val partitionKeyValues4 = Map[String, String](datePartitionName -> "2018-08-31-17-57", batchIdPartitionName -> "1535727420003").asJava
  val partitionKeyValues5 = Map[String, String](datePartitionName -> "2018-08-31-15-42", batchIdPartitionName -> "1535719375004").asJava
  val partitionKeyValues6 = Map[String, String](datePartitionName -> "2018-08-30-10-25", batchIdPartitionName -> "1535613940012").asJava
  val partitionKeyValues7 = Map[String, String](datePartitionName -> "2018-08-30-10-25", batchIdPartitionName -> "1535613930061").asJava
  val path = "path/file"
  // val mapOfTypes = Map(datePartitionName -> "string", batchIdPartitionName -> "bigint")
}

