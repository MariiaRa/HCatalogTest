import java.util.Date

import com.ua.HiveMetastoreClient
import org.apache.hadoop.conf.Configuration
import org.mockito.Mockito.when
import org.scalatest.{FunSuite, MustMatchers}
import org.scalatest.mockito.MockitoSugar

class HiveMetastoreClientSuite extends FunSuite with MustMatchers with MockitoSugar {

  private val format = "yyyy-MM-dd"
  private val dateFormat = new java.text.SimpleDateFormat(format)

  private val expectedMaxBatchId = 1002L
  private val expectedBatchIdRangeResult = List(100L, 200L)
  private val expectedDate: Date = dateFormat.parse("1900-01-01")
  private val expectedMap = Map[Any, Any](expectedDate -> 2L)

  private val config = mock[Configuration]
  private val myClient = mock[HiveMetastoreClient]

  test("getMaxBatchId(valid table) returns max batch id") {
    when(myClient.getMaxBatchId("database", "table")).thenReturn(expectedMaxBatchId)
    val actualMaxBatchId = myClient.getMaxBatchId("database", "table")
    assert(expectedMaxBatchId === actualMaxBatchId)
  }

  test("getMaxBatchRange(valid table, from batchId) returns max batchId range") {
    when(myClient.getMaxBatchIdRange("database", "table", 50L)).thenReturn(expectedBatchIdRangeResult)
    val actualMaxBatchIdrange = myClient.getMaxBatchIdRange("database", "table", 50L)
    assert(expectedBatchIdRangeResult === actualMaxBatchIdrange)
  }

  test("getMaxDate(valid table, partitionKey) returns max date") {
    when(myClient.getMaxDate("database", "table", "dateid", format)).thenReturn(expectedDate)
    val actualMaxDate = myClient.getMaxDate("database", "table", "dateid", format)
    assert(expectedDate === actualMaxDate)
  }

  test("getDateRange(valid table, from batch) returns map with dates and max batchId") {
    when(myClient.getDateRange("database", "table", 1L, format)).thenReturn(expectedMap)
    val actualMap = myClient.getDateRange("database", "table", 1L, format)
    assert(expectedMap === actualMap)
  }
}
