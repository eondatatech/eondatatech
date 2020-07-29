package com.eon.data

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkJob {

  private val logger = LoggerFactory.getLogger(getClass.getName)
  var spark: SparkSession = _

  def createSparkSession(): Unit = {
    var conf = new SparkConf()
      .setAppName("SparkJob")
      //.setMaster("local[*]")
      .setIfMissing("hive.execution.engine", "spark")
    spark = SparkSession
      .builder()
      .enableHiveSupport()
      //.config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .config(conf)
      .getOrCreate()
    spark
  }

  def getSparkSession(): SparkSession = {
    if ((spark ne null) && !spark.sparkContext.isStopped) {
      return spark
    } else {
      this.createSparkSession()
      return spark
    }
  }

  def main(args: Array[String]): Unit = {
    //create spark session
    println("============Create spark session=====-START")
    this.createSparkSession()
    EonFactory.dataProcessor(args(0),args(1),args(2))
    println("============Create spark session=====-END")
  }
}
