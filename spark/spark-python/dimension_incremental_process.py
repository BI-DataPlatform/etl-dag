import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, asc, monotonically_increasing_id

def run(argv):
    """
    :args:
    --src-table     원천 테이블명
    --dst-table     가공 후 타깃 테이블명
    --partition-col partition 컬럼명
    --partition     작업 partition
    --row-kind-col  로그 유형 컬럼명
    --index-col     인덱스 컬럼명
    --start         시작 인덱스
    --end           종료 인덱스
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--src-table', dest='src_table', type=str, required=True)
    parser.add_argument('--dst-table', dest='dst_table', type=str, required=True)
    parser.add_argument('--partition-col', dest='partition_col', type=str)
    parser.add_argument('--partition', dest='partition', type=str)
    parser.add_argument('--row-kind-col', dest='rowkind_col', type=str, required=True)
    parser.add_argument('--index-col', dest='index_col', type=str)
    parser.add_argument('--start', dest='start_index', type=int)
    parser.add_argument('--end', dest='end_index', type=int)

    try:
        args = parser.parse_args()
    except Exception as e:
        parser.print_help()
        sys.exit(2)

    appname = "prepare_pipeline"

    spark = SparkSession.builder.appName(appname) \
        .getOrCreate()

    # dataframe to process
    df = spark.table(args.src_table)
    if args.partition_col:
        df = df.filter(col(args.partition_col) == args.partition)
    if args.index_col:
        if args.start_index is not None or args.start_index != 0:
            df = df.filter((col(args.index_col) >= args.start_index))
        if args.end_index is not None:
            df = df.filter((col(args.index_col) < args.end_index))
    df = df.orderBy(asc(args.index_col))

    df.show()

    # TODO: append audit dim


    # append dim
    for row in df.collect():
        # TODO: quality filter
        row_kind = row[args.row_kind_col]
        df = df.drop(args.index_col, args.row_kind_col)
        if row_kind == 'INSERT':
            df.writeTo(args.dst_table).append()
        elif row_kind == 'UPDATE':


run(sys.argv)
