package io.malachai.etltool.process

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object HistoricLoadProcess {

  def run(args: Map[String, Any]) = {

    val map = args.asInstanceOf[Map[String, String]]
    val appname = "historic_load_process"

    val spark = SparkSession.builder.appName(appname).getOrCreate()

    var df = spark.table(map("src_table"))

    df = df.filter(df(map("timestamp_col")).cast("long") >= lit(map("ts_from").toLong))
      .filter(df(map("timestamp_col")).cast("long") <= lit(map("ts_to").toLong))
      .orderBy(asc(map("timestamp_col")))

    // appending partition key
    df = df.withColumn(map("partition_col"), lit(args("partition")))

    // appending surrogate keys
    if (map.contains("surrogate_col")) {
      df = df.withColumn(map("surrogate_col"), monotonically_increasing_id())
    }

    // overwrite partition
    df.writeTo(map("dst_table")).overwritePartitions()

  }

}
