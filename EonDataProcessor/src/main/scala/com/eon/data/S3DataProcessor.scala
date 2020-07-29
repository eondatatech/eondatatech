package com.eon.data

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

object S3DataProcessor extends Processor {

  private val logger = LoggerFactory.getLogger(getClass.getName)
  private var spark = null


  override def read(): Unit = {
    val spark = SparkJob.getSparkSession()
    // Replace Key with your AWS account key (You can find this on IAM
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.access.key", "")

    // Replace Key with your AWS secret key (You can find this on IAM
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.secret.key", "")

    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.impl", "fs.s3a.S3AFileSystem")
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
    spark.sparkContext.setLogLevel("ERROR")

    logger.info("##spark read text files from a directory into RDD")

    val rddFromFile = spark.read.csv("s3://eondataemrbucket/survey_results_public.csv")
    println(rddFromFile.getClass)

    logger.info("##Get data Using collect")
    rddFromFile.collect().foreach(record => {
      println(record

      )
    })

    logger.info("==================Start read operation=======================")
    val df: DataFrame = spark.read.text("s3://eondataemrbucket/survey_results_public.csv")
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
