

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkS3DataProcessor {

  def readFileFromS3(): Unit = {

    var conf = new SparkConf()
      .setAppName("SparkAWSPOC")
             //.setMaster("local[*]")
      .setIfMissing("hive.execution.engine", "spark")
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      //.config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .config(conf)
      .getOrCreate()

    // Replace Key with your AWS account key (You can find this on IAM
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.access.key", "AKIA6N4QTQFKESECNAFI")

    // Replace Key with your AWS secret key (You can find this on IAM
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.secret.key", "WTrvLLQJ0wv2pD0rs0RiV/2jaHlsOGaAS+gFWryy")

    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.impl","fs.s3a.S3AFileSystem")
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
    spark.sparkContext.setLogLevel("ERROR")

    println("##spark read text files from a directory into RDD")

    val rddFromFile = spark.read.csv("s3://eondataemrbucket/survey_results_public.csv")
    println(rddFromFile.getClass)

    println("##Get data Using collect")
    rddFromFile.collect().foreach(record => {
      println(record

      )
    })

    println("==================Start read operation=======================")
    val df: DataFrame = spark.read.text("s3://eondataemrbucket/survey_results_public.csv")
    println("=================PrintSchema======================")
    df.printSchema()
    println("=================Dataframe show======================")
    df.show(false)
    println("=================Count======================")
    println(df.count())
    println("=================End======================")


  }

  def main(args: Array[String]): Unit = {
    //create spark session

    println("============Read Data from S3 bucket- START=======================")
    readFileFromS3()
    println("============Read Data from S3 bucket- END=======================")
  }
}
