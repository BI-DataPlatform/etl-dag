package io.malachai.etltool.process.load

import org.apache.spark.sql.{DataFrame, SparkSession}

trait DimensionLoader {

  def load(spark: SparkSession, df: DataFrame, map: Map[String, String])

}
