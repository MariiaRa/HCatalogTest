package com.ua

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hive.hcatalog.api.HCatClient

import scala.collection.JavaConverters._

class HCatalogClient {

  private def createHCatClient(file: String): HCatClient = {
    val config = new Configuration()
    config.addResource(new Path(file))
    HCatClient.create(config)
  }

  def getPartitionValues(dbName: String, tableName: String, file: String): List[String] = {
    val hCatClient = createHCatClient(file)
    val partitons = hCatClient.getPartitions(dbName, tableName).asScala
    partitons.map(p => p.getValues.asScala.toList).toList.flatten
  }

}
