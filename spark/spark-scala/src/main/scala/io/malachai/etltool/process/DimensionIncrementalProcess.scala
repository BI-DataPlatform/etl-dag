package io.malachai.etltool.process

import io.malachai.etltool.process.load.DimensionLoaderFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DimensionIncrementalProcess {

  def run(kwargs: Map[String, Any], args: List[String]) = {
    val map = kwargs.asInstanceOf[Map[String, String]]
    val appname = "dimension_incremental_process"

    val spark = SparkSession.builder.appName(appname).getOrCreate()

    var df = spark.table(map("src_table"))

    if (map.contains("partition_col")) {
      df = df.filter(df(map("partition_col")) === lit(map("partition")))
    }

    if (map.contains("index_col")) {
      df = df.filter(df(map("index_col")) >= lit(map("start_index")))
      if (map.contains("end_index")) {
        df = df.filter(df(map("index_col")) < lit(map("end_index")))
      }
    }

    df = df.orderBy(asc(map("index_col")))

    df.show()

    // TODO: append audit dim

    DimensionLoaderFactory.getInstance(args.head).load(spark, df, map)


  }
}

