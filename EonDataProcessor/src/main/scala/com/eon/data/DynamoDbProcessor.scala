package com.eon.data
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

object DynamoDbProcessor extends Processor {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  override def read(accessKey:String, secretKey:String): Unit = {}

  override def transform(dataFrame: DataFrame): Unit = {}

  override def write(outputDataFrame: DataFrame): Unit = {}
}
