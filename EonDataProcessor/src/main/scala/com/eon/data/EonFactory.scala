package com.eon.data

import org.slf4j.LoggerFactory

object EonFactory {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  def dataProcessor(dataSource:String, accessKey:String, secretKey:String):Unit={
    logger.info("===========Factory operation-START")
    dataSource match {
      case "S3" => {
        logger.info("===========S3 operation-START")
        S3DataProcessor.read(accessKey, secretKey)
        logger.info("===========S3 operation-END")
      }
      case "DYNAMO_DB" => {
        logger.info("===========Dynamo DB operation-START")
        DynamoDbProcessor.read(accessKey, secretKey)
        logger.info("===========Dynamo DB operation-END")
      }
      case _ => throw new RuntimeException(s"There is no data source passed!!!")
    }
    logger.info("===========Factory operation-END")
  }
}
