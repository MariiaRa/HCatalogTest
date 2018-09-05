package com.ua


import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hive.hcatalog.api.{HCatPartition, HCatTable}
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema.Type

import scala.collection.JavaConverters._


object TestHCatClient {
  val batchIdPartitionName: String = "batch_id"
  val datePartitionName: String = "date"
  val partColumnsNames = List(datePartitionName, batchIdPartitionName)
  val partitionKeyValues1 = Map[String, String](datePartitionName -> "2018-08-30-10-25", batchIdPartitionName -> "1535613940012").asJava
  val partitionKeyValues2 = Map[String, String](datePartitionName -> "2018-08-31-17-57", batchIdPartitionName -> "1535727475002").asJava
  val partitionKeyValues3 = Map[String, String](datePartitionName -> "2018-08-31-17-57", batchIdPartitionName -> "1535727445003").asJava
  val partitionKeyValues4 = Map[String, String](datePartitionName -> "2018-08-31-17-57", batchIdPartitionName -> "1535727420003").asJava
  val partitionKeyValues5 = Map[String, String](datePartitionName -> "2018-08-31-15-42", batchIdPartitionName -> "1535719375004").asJava
  val path = "path/file"
  val mapOfTypes = Map(datePartitionName -> "string", batchIdPartitionName -> "bigint")
}

class TestHCatClient {

  import TestHCatClient._

  private def createTable(): HCatTable = {
    val table: HCatTable = new HCatTable("database", "table")
    val colOne = new HCatFieldSchema("id", Type.STRING, null)
    val colTwo = new HCatFieldSchema("value", Type.DECIMAL, null)
    val columns = List(colOne, colTwo).asJava
    table.cols(columns)
    val colPartOne = new HCatFieldSchema(datePartitionName, Type.STRING, null)
    val colPartTwo = new HCatFieldSchema(batchIdPartitionName, Type.STRING, null)
    val columnsPart = List(colPartOne, colPartTwo).asJava
    table.partCols(columnsPart)
  }


  private def getPartition(): java.util.List[HCatPartition] = {
    val table = createTable()
    val partition1 = new HCatPartition(table, partitionKeyValues1, path)
    val partition2 = new HCatPartition(table, partitionKeyValues2, path)
    val partition3 = new HCatPartition(table, partitionKeyValues3, path)
    val partition4 = new HCatPartition(table, partitionKeyValues4, path)
    val partition5 = new HCatPartition(table, partitionKeyValues5, path)
    List[HCatPartition](partition1, partition2, partition3, partition4, partition5).asJava
  }


  def getMaxBatchId(databaseName: String, tableName: String): Long = {
    val partitionValues = getPartition().asScala.map(_.getValues.asScala.toList).toList

    val columnsData = partitionValues
      .flatMap(value => value.zip(partColumnsNames))
      .filter { case (value, columnName) => columnName == batchIdPartitionName }
      .map { case (value, columnName) => value.toLong }

    columnsData.max
  }

  def getMaxBatchIdRange(databaseName: String, tableName: String, fromBatchId: Long): List[Long] = {
    val partitionValues = getPartition().asScala.map(_.getValues.asScala.toList).toList

    val columnsData = partitionValues
      .flatMap(value => value.zip(partColumnsNames))
      .filter { case (value, columnName) => columnName == batchIdPartitionName }
      .map { case (value, columnName) => value.toLong }
      .dropWhile(value => !(value == fromBatchId))

    columnsData.sortWith(_ < _)
  }

  def getMaxDate(databaseName: String, tableName: String, partitionName: String, format: String): Date = {
    val partitionValues: List[List[String]] = getPartition().asScala.map(_.getValues.asScala.toList).toList
    val dateFormat = new SimpleDateFormat(format)

    val columnsData = partitionValues
      .flatMap(value => value.zip(partColumnsNames))
      .filter { case (value, columnName) => columnName == datePartitionName }
      .map { case (value, columnName) => dateFormat.parse(value) }
    println(columnsData)
    columnsData.max
  }

  def getDateRange(databaseName: String, tableName: String, fromBatchId: Long, format: String) = {
    val partitionValues: List[List[String]] = getPartition().asScala.map(_.getValues.asScala.toList).toList

    val convertedList = convertColumnType(databaseName: String, tableName: String, partitionValues, format)
      .sortWith(_.last.asInstanceOf[Long] < _.last.asInstanceOf[Long])
      .dropWhile(list => !list.contains(fromBatchId)).map(list => list.head -> list.last).toMap

    convertedList
  }

  private def convertColumnType(databaseName: String, tableName: String, list: List[List[String]], format: String) = {

    val dateFormat = new SimpleDateFormat(format)

    list.map(listOfValues => listOfValues.zip(partColumnsNames)).map { listOfTuples =>
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
