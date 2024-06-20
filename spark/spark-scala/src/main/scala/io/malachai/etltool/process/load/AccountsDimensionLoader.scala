package io.malachai.etltool.process.load

import org.apache.spark.sql.{DataFrame, SparkSession}

class AccountsDimensionLoader extends DimensionLoader {

  def load(spark: SparkSession, df: DataFrame, map: Map[String, String]): Unit = {
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
