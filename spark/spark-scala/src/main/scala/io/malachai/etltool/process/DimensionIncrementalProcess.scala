package io.malachai.etltool.process

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DimensionIncrementalProcess {

  def run(args: Map[String, Any]) = {
    val map = args.asInstanceOf[Map[String, String]]
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


    // append dim
    df.createOrReplaceTempView("temp")
    spark.sql(s"""
        |MERGE INTO ${map("dst_table")} t
        |USING (SELECT * FROM temp) s
        |ON t.id = s.id AND s.rowkind = 'UPDATE' AND t.row_indicator=TRUE
        |WHEN MATCHED THEN UPDATE SET t.row_indicator=FALSE, t.expiration_timestamp=NOW()
        |""".stripMargin
    )
    spark.sql(s"""
         |INSERT INTO ${map("dst_table")}
         |VALUES
         |""".stripMargin)


    df.collect().foreach(row => {
      // TODO: quality filter
      val rowKind = row.get(row.fieldIndex(map("row_kind_col")))
      if (rowKind == "INSERT") {
        spark.sql(s"INSERT INTO ${map("src_table")} ")
      } else if (rowKind == "UPDATE") {

      }
    })

    df = df.drop(map("index_col"), map("row_kind_col"))
  }
}
