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

    val columns = Seq("id", "email", "nickname", "phone_number", "virtual_number", "payment", "family_account_id", "points", "rank", "role", "created_on", "last_updated_on")
    // append dim
    df.createOrReplaceTempView("temp")
    df.collect().foreach(row => {
      // TODO: quality filter
      val rowKind = row.get(row.fieldIndex(map("row_kind_col")))
      if (rowKind == "INSERT") {
        spark.sql(s"""
          |INSERT INTO ${map("dst_table")}
          |(${columns.mkString(",")}, row_effective, row_expiration, row_indicator)
          |VALUES(${columns.map(c => c -> row.get(row.fieldIndex(c))).map(mapAccountsDim).mkString(",")}, NOW(), TIMESTAMP('9999-12-31 00:00:00'), TRUE)
          |""".stripMargin
        )
      } else if (rowKind == "UPDATE") {
        spark.sql(s"""
          |UPDATE ${map("dst_table")}
          |SET row_indicator=FALSE, row_expiration=NOW()
          |WHERE id="${row.get(row.fieldIndex("id"))}" AND row_indicator=TRUE
          |""".stripMargin
        )
        spark.sql(s"""
          |INSERT INTO ${map("dst_table")}
          |(${columns.mkString(",")}, row_effective, row_expiration, row_indicator)
          |VALUES(${columns.map(c => c -> row.get(row.fieldIndex(c))).map(mapAccountsDim).mkString(",")}, NOW(), TIMESTAMP('9999-12-31 00:00:00'), TRUE)
          |""".stripMargin
        )
      }
    })
    spark.sql(s"SELECT ${columns.mkString(",")} FROM ${map("dst_table")}").show()
  }

  private def mapAccountsDim(t: Tuple2[String, Any]): Any = t._1 match {
      case _ if t._2==null => "NULL"
      case "id" | "email" | "nickname" | "phone_number" | "virtual_number"
                | "payment" | "family_account_id" | "rank" | "role" => "'"+t._2+"'"
      case "points" => t._2.toString.toLong
      case "created_on" | "last_updated_on" => "TIMESTAMP('"+t._2+"')"
  }
}

