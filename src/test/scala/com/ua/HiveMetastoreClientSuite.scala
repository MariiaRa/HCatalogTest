package com.ua

import java.time.{Instant, ZoneId}

import org.scalatest.FunSuite

class HiveMetastoreClientSuite extends FunSuite {

  private val databaseName = "database"
  private val tableName = "table"
  private val format = "yyyy-MM-dd"
  private val dateFormat = new java.text.SimpleDateFormat(format)


  private val expectedMaxBatchId = 1535727475002L
  private val expectedBatchIdRange = List(1535613930061L, 1535613940012L, 1535719375004L, 1535727420003L)
  private val expectedBatchIdRangeWithFilter = List(1535613930061L, 1535613940012L)
  private val expectedDate = Instant.ofEpochMilli(
    dateFormat.parse("2018-08-31").getTime
  ).atZone(ZoneId.systemDefault()).toLocalDate

  private val expectedMap = Map[Any, Any](dateFormat.parse("2018-08-30") -> 1535613940012L, dateFormat.parse("2018-08-31") -> 1535727475002L)
  private val expectedMapWithFilter = Map[Any, Any](dateFormat.parse("2018-08-31") -> 1535727475002L)
  private val expectedBatchIdRangeNoFilter = List(1535613930061L, 1535613940012L, 1535719375004L, 1535727420003L)

  private val myHCatclient = new TestHCatClient(databaseName, tableName)

  test("getMaxBatchId(valid table) returns max batchId") {
    val actualMaxBatchId = myHCatclient.getMaxBatchId
    assert(expectedMaxBatchId === actualMaxBatchId)
  }

  test("getMaxBatchRange(valid table, from batchId) returns max batchId range from filtered partitons") {
    val actualBatchIdRangeResult = myHCatclient.getBatchIdRange(1535727420003L, Some("date", "2018-08-30-10-25"))
    assert(expectedBatchIdRangeWithFilter === actualBatchIdRangeResult)
  }

  test("getMaxBatchRange(valid table, from batchId) returns max batchId range") {
    val actualBatchIdRangeResult = myHCatclient.getBatchIdRange(1535727420003L)
    assert(expectedBatchIdRange === actualBatchIdRangeResult)
  }

  test("getMaxPartitionId(valid table, partitionName, filterKey, filterValue) returns max batchId in partition") {
    val actualMaxPartitionIdResult = myHCatclient.getMaxPartitionId("batch_id", "date", "2018-08-31-17-57")
    assert(expectedMaxBatchId === actualMaxPartitionIdResult)
  }

  test("getMaxPartitionId(valid table, partitionName) returns max batchId in partition") {
    val actualMaxPartitionIdResult = myHCatclient.getMaxPartitionId("batch_id")
    assert(expectedMaxBatchId === actualMaxPartitionIdResult)
  }

  test("getMaxPartitionId(valid table, partitionName, filter) returns max batchId in partition with filter") {
    val actualBatchIdRangeResult = myHCatclient.getMaxPartitionIdRange(1535727420003L, "batch_id", Some("date", "2018-08-30-10-25"))
    assert(expectedBatchIdRangeWithFilter === actualBatchIdRangeResult)
  }

  test("getMaxPartitionId(valid table, partitionName, filter=None) returns max batchId in partition") {
    val actualBatchIdRangeResult = myHCatclient.getMaxPartitionIdRange(1535727420003L, "batch_id", None)
    assert(expectedBatchIdRangeNoFilter === actualBatchIdRangeResult)
  }

  test("getMaxDate(valid table, partitionName) returns max date") {
    val actualMaxDate = myHCatclient.getMaxDate("date")
    assert(Some(expectedDate) === actualMaxDate)
  }

  test("getDateRange(valid table, from batchId, filterKey, filterValue) returns map with dates and max batchIds in filtered partition") {
    val actualMap = myHCatclient.getDateRange(1535727475002L, "date", "2018-08-31-17-57")
    assert(expectedMapWithFilter === actualMap)
  }

  test("getDateRange(valid table, from batchId) returns map with dates and max batchIds") {
    val actualMap = myHCatclient.getDateRange(1535727475002L)
    assert(expectedMap === actualMap)
  }
}
