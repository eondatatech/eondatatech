package com.eon.data

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

object S3DataProcessor extends Processor {

  private val logger = LoggerFactory.getLogger(getClass.getName)
  private var spark = null
  private val bucketPath = "s3://eondataemrbucket/survey_results_public.csv"

  override def read(accessKey:String, secretKey:String): Unit = {
    val spark = SparkJob.getSparkSession()
    // Replace Key with your AWS account key (You can find this on IAM
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.access.key", accessKey)

    // Replace Key with your AWS secret key (You can find this on IAM
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.secret.key", secretKey)

    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.impl", "fs.s3a.S3AFileSystem")
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
    spark.sparkContext.setLogLevel("ERROR")

    logger.info("##spark read text files from a directory into RDD")

    val rddFromFile = spark.read.csv(bucketPath)
    println(rddFromFile.getClass)

    logger.info("##Get data Using collect")
    rddFromFile.collect().foreach(record => {
      println(record

      )
    })

    logger.info("==================Start read operation=======================")
    val df: DataFrame = spark.read.text(bucketPath)
    logger.info("=================PrintSchema======================")
    df.printSchema()
    logger.info("=================Dataframe show======================")
    df.show(false)
    logger.info("=================Count======================")
    println(df.count())
    logger.info("=================End======================")
  }

  override def transform(dataFrame: DataFrame): Unit = {}

  override def write(outputDataFrame: DataFrame): Unit = ???
}
