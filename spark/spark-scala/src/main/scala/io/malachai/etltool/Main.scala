package io.malachai.etltool

import io.malachai.etltool.process.{DimensionIncrementalProcess, HistoricLoadProcess}

import scala.annotation.tailrec
import scala.sys.exit

object Main {

  val usage =
    """
      |Usage:
      |historic-load [--arg1 num] [--arg2 str] arg3
      |dimension-incremental [--arg1 num] [--arg2 str]
      |""".stripMargin

  def main(args: Array[String]): Unit = {

    println(args.mkString("/"))
    if (args.length == 0) {
      println(usage)
      exit(0)
    }
    else if (args(0) == "historic-load") {
      val help = """
        |    --partition     partition key의 값 (일반적으로 작업의 기준 DateStamp)
        |    --partition-col partitioning 컬럼명
        |    --src-table     원천 테이블명
        |    --dst-table     가공 후 타깃 테이블명
        |    --timestamp-col 데이터를 정렬하는 기준 timestamp 컬럼명
        |    --from          timestamp-col 값 중 데이터 추출 시작점을 기준하는 정수 값
        |    --to            timestamp-col 값 중 데이터 추출 끝점을 기준하는 정수 값
        |    --surrogate-key 생성할 대체키 컬럼명
        |""".stripMargin
      @tailrec
      def argParse(kwargs: Map[String, Any], args: List[String], list: List[String]): (Map[String, Any], List[String]) = {
        list match {
          case Nil => kwargs -> args
          case "--partition" :: value :: tail =>
            argParse(kwargs ++ Map("partition" -> value), args, tail)
          case "--partition-col" :: value :: tail =>
            argParse(kwargs ++ Map("partition_col" -> value), args, tail)
          case "--src-table" :: value :: tail =>
            argParse(kwargs ++ Map("src_table" -> value), args, tail)
          case "--dst-table" :: value :: tail =>
            argParse(kwargs ++ Map("dst_table" -> value), args, tail)
          case "--timestamp-col" :: value :: tail =>
            argParse(kwargs ++ Map("timestamp_col" -> value), args, tail)
          case "--from" :: value :: tail =>
            argParse(kwargs ++ Map("ts_from" -> value), args, tail)
          case "--to" :: value :: tail =>
            argParse(kwargs ++ Map("ts_to" -> value), args, tail)
          case "--surrogate-col" :: value :: tail =>
            argParse(kwargs ++ Map("surrogate_col" -> value), args, tail)
          case string :: _ if !string.startsWith("--") =>
            argParse(kwargs, args :+ string, list.tail)
          case unknown :: _ =>
            println("Unknown argument: " + unknown)
            println(help)
            exit(1)
        }
      }
      def argDefault(map: Map[String, Any]): Map[String, Any] = {
        map ++ "--ts-from" -> "0"
        map ++ "--ts-to" -> "0"
        map
      }
      val opt = argParse(argDefault(Map()), List(), args.toList.tail)
      println(opt)

      HistoricLoadProcess.run(opt._1)

    } else if (args(0) == "dimension-incremental") {
      val help = """
        |    --src-table     원천 테이블명
        |    --dst-table     가공 후 타깃 테이블명
        |    --partition-col partition 컬럼명
        |    --partition     작업 partition
        |    --row-kind-col  로그 유형 컬럼명
        |    --index-col     인덱스 컬럼명
        |    --start         시작 인덱스
        |    --end           종료 인덱스
        |""".stripMargin
      @tailrec
      def argParse(kwargs: Map[String, Any], args: List[String], list: List[String]): (Map[String, Any], List[String]) = {
        list match {
          case Nil => kwargs -> args
          case "--partition" :: value :: tail =>
            argParse(kwargs ++ Map("partition" -> value), args, tail)
          case "--partition-col" :: value :: tail =>
            argParse(kwargs ++ Map("partition_col" -> value), args, tail)
          case "--src-table" :: value :: tail =>
            argParse(kwargs ++ Map("src_table" -> value), args, tail)
          case "--dst-table" :: value :: tail =>
            argParse(kwargs ++ Map("dst_table" -> value), args, tail)
          case "--row-kind-col" :: value :: tail =>
            argParse(kwargs ++ Map("row_kind_col" -> value), args, tail)
          case "--index-col" :: value :: tail =>
            argParse(kwargs ++ Map("index_col" -> value), args, tail)
          case "--start" :: value :: tail =>
            argParse(kwargs ++ Map("start_index" -> value), args, tail)
          case "--end" :: value :: tail =>
            argParse(kwargs ++ Map("end_index" -> value), args, tail)
          case string :: _ if !string.startsWith("--") =>
            argParse(kwargs, args :+ string, list.tail)
          case unknown :: _ =>
            println("Unknown argument: " + unknown)
            println(help)
            exit(1)
        }
      }
      def argDefault(map: Map[String, Any]): Map[String, Any] = {
        map
      }

      val opt = argParse(argDefault(Map()), List(), args.toList.tail)
      println(opt)

      DimensionIncrementalProcess.run(opt._1)

    } else {
      println("unknown command: " + args(0))
      exit(1)
    }



  }
}
