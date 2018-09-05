package com.ua

import java.util.Date

import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FunSuite, MustMatchers}

class HiveMetastoreClientSuite extends FunSuite with MustMatchers with MockitoSugar {

  private val format = "yyyy-MM-dd"
  private val dateFormat = new java.text.SimpleDateFormat(format)

  private val expectedMaxBatchId = 1535727475002L
  private val expectedBatchIdRangeResult = List(1535719375004L, 1535727420003L, 1535727445003L, 1535727475002L)
  private val expectedDate: Date = dateFormat.parse("2018-08-31")
  private val expectedMap = Map[Any, Any](expectedDate -> 1535727475002L)

  private val myHCatclient = new TestHCatClient


  test("getMaxBatchId(valid table) returns max batchId") {
    val actualMaxBatchId = myHCatclient.getMaxBatchId("database", "table")
    assert(expectedMaxBatchId === actualMaxBatchId)
  }

  test("getMaxBatchRange(valid table, from batchId) returns max batchId range") {
    val actualBatchIdRangeResult = myHCatclient.getMaxBatchIdRange("database", "table", 1535727475002L)
    assert(expectedBatchIdRangeResult === actualBatchIdRangeResult)
  }

  test("getMaxDate(valid table, partitionName) returns max date") {
    val actualMaxDate = myHCatclient.getMaxDate("database", "table", "date", format)
    assert(expectedDate === actualMaxDate)
  }

  test("getDateRange(valid table, from batchId) returns map with dates and max batchIds") {
    val actualMap = myHCatclient.getDateRange("database", "table", 1535727475002L, format)
    assert(expectedMap === actualMap)
  }
}
