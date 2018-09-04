package com.ua


import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hive.hcatalog.api.{HCatPartition, HCatTable}
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema.Type

import scala.collection.JavaConverters._


object TestHCatClient {
  val columns = List("dateid", "rddid")
  val partitionKeyValuesOne = Map[String, String]("dateid" -> "2018-08-30-10-25", "rddid" -> "1535613940012").asJava
  val partitionKeyValuesTwo = Map[String, String]("dateid" -> "2018-08-31-17-57", "rddid" -> "1535727475002").asJava
  val partitionKeyValuesThree = Map[String, String]("dateid" -> "2018-08-31-17-57", "rddid" -> "1535727445003").asJava
  val partitionKeyValuesFour = Map[String, String]("dateid" -> "2018-08-31-17-57", "rddid" -> "1535727420003").asJava
  val defaultPartitionName: String = "rddid"
}

class TestHCatClient {

  import TestHCatClient._

  private def createTable(): HCatTable = {
    val table: HCatTable = new HCatTable("database", "table")
    val colOne = new HCatFieldSchema("id", Type.STRING, null)
    val colTwo = new HCatFieldSchema("value", Type.DECIMAL, "value columns")
    val columns = List(colOne, colTwo).asJava
    table.cols(columns)
    val colPartOne = new HCatFieldSchema("dateid", Type.STRING, null)
    val colPartTwo = new HCatFieldSchema("rddid", Type.STRING, null)
    val columnsPart = List(colPartOne, colPartTwo).asJava
    table.partCols(columnsPart)
  }


  private def getPartition(): java.util.List[HCatPartition] = {
    val table = createTable()
    val partitionOne = new HCatPartition(table, partitionKeyValuesOne, "file/path")
    val partitionTwo = new HCatPartition(table, partitionKeyValuesTwo, "file/path")
    val partitionThree = new HCatPartition(table, partitionKeyValuesThree, "file/path")
    val partitionFour = new HCatPartition(table, partitionKeyValuesFour, "file/path")
    List[HCatPartition](partitionOne, partitionTwo, partitionThree, partitionFour).asJava
  }


  def getMaxBatchId(databaseName: String, tableName: String): Long = {
    val partitionValues = getPartition().asScala.map(_.getValues.asScala.toList).toList

    val columnsData = partitionValues
      .flatMap(value => value.zip(TestHCatClient.columns))
      .filter { case (value, columnName) => columnName == defaultPartitionName }
      .map { case (value, columnName) => value.toLong }

    columnsData.max
  }

  def getMaxBatchIdRange(databaseName: String, tableName: String, fromBatchId: Long): List[Long] = {
    val partitionValues = getPartition().asScala.map(_.getValues.asScala.toList).toList

    val columnsData = partitionValues
      .flatMap(value => value.zip(TestHCatClient.columns))
      .filter { case (value, columnName) => columnName == defaultPartitionName }
      .map { case (value, columnName) => value.toLong }
      .dropWhile(value => !(value == fromBatchId))

    columnsData.sortWith(_ < _)
  }

  def getMaxDate(databaseName: String, tableName: String, partitionName: String, format: String): Date = {
    val partitionValues: List[List[String]] = getPartition().asScala.map(_.getValues.asScala.toList).toList
    val dateFormat = new SimpleDateFormat(format)

    val columnsData = partitionValues
      .flatMap(value => value.zip(TestHCatClient.columns))
      .filter { case (value, columnName) => columnName == partitionName }
      .map { case (value, columnName) => dateFormat.parse(value) }

    columnsData.max
  }

  def getDateRange(databaseName: String, tableName: String, fromBatchId: Long, format: String) = {
    val partitionValues: List[List[String]] = getPartition().asScala.map(_.getValues.asScala.toList).toList

    val convertedList = convertColumnValue(databaseName: String, tableName: String, partitionValues, format)
      .sortWith(_.last.asInstanceOf[Long] < _.last.asInstanceOf[Long])
      .dropWhile(list => !list.contains(fromBatchId)).map(list => list.head -> list.last).toMap

    convertedList
  }

  private def convertColumnValue(databaseName: String, tableName: String, list: List[List[String]], format: String) = {
    val mapOfTypes = Map("dateid" -> "string", "rddid" -> "bigint")
    val dateFormat = new SimpleDateFormat(format)

    list.map(listOfValues => listOfValues.zip(TestHCatClient.columns)).map { listOfTuples =>
      listOfTuples.map { case (value, columnName) =>
        mapOfTypes(columnName) match {
          case "string" => dateFormat.parse(value) //should be date in hive table
          case "bigint" => value.toLong
          case _ => value
        }
      }
    }
  }
}
