package com.eon.data

import org.apache.spark.sql.DataFrame

trait Processor {
  def read(accessKey:String, secretKey:String)
  def transform(dataFrame: DataFrame)
  def write(outputDataFrame: DataFrame)
}
