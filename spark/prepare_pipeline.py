import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, asc, monotonically_increasing_id


def run(argv):
    """
    :args:
    --partition     partition key의 값 (일반적으로 작업의 기준 DateStamp)
    --partition-col partitioning 컬럼명
    --src-table     원천 테이블명
    --dst-table     가공 후 타깃 테이블명
    --timestamp-col 데이터를 정렬하는 기준 timestamp 컬럼명
    --from          timestamp-col 값 중 데이터 추출 시작점을 기준하는 정수 값
    --to            timestamp-col 값 중 데이터 추출 끝점을 기준하는 정수 값
    --surrogate-key 생성할 대체키 컬럼명
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--src-table', dest='src_table', type=str, required=True)
    parser.add_argument('--dst-table', dest='dst_table', type=str, required=True)
    parser.add_argument('--partition', dest='partition', type=str, required=True)
    parser.add_argument('--partition-col', dest='partition_col', type=str, required=True)
    parser.add_argument('--timestamp-col', dest='timestamp_col', type=str, required=True)
    parser.add_argument('--from', dest='ts_from', type=int, default=0)
    parser.add_argument('--to', dest='ts_to', type=int, default=0)
    parser.add_argument('--surrogate-col', dest='surrogate_col', type=str)
    try:
        args = parser.parse_args()
    except Exception as e:
        parser.print_help()
        sys.exit(2)

    appname = "prepare_pipeline"

    spark = SparkSession.builder.appName(appname) \
        .getOrCreate()

    # dataframe to process
    df = spark.table(args.src_table) \
        .filter(
        (args.ts_from <= col(args.timestamp_col).cast('long')) & (col(args.timestamp_col).cast('long') <= args.ts_to)) \
        .orderBy(asc(args.timestamp_col))

    # appending partition key
    df = df.withColumn(args.partition_col, lit(args.partition))

    # appending surrogate keys
    if args.surrogate_col:
        df = df.withColumn(args.surrogate_col, monotonically_increasing_id())

    # overwrite partition
    df.writeTo(args.dst_table).overwritePartitions()


run(sys.argv)
