from pyspark.sql import DataFrame, SparkSession, SQLContext


def tables(spark: SparkSession, client: str) -> DataFrame:
    """Get names of the Accumulo tables.

    :param spark: Spark session
    :param client: Accumulo client properties filepath
    :return: DataFrame
    """
    scala_utils = spark.sparkContext._jvm.com.microsoft.ai.accumulo.ScalaUtils

    _jdf = scala_utils.tables(
        spark._jsparkSession,
        client,
    )
    return DataFrame(_jdf, SQLContext.getOrCreate(spark.sparkContext))


def table_head(spark: SparkSession, client: str, table: str, num_rows: int = 1) -> DataFrame:
    """Get first n rows from the table.

    :param spark: Spark session
    :param client: Accumulo client properties filepath
    :param table: Accumulo table name
    :param num_rows: Number of rows to retrieve
    :return: DataFrame
    """
    scala_utils = spark.sparkContext._jvm.com.microsoft.ai.accumulo.ScalaUtils

    _jdf = scala_utils.tableHead(
        spark._jsparkSession,
        client,
        table,
        num_rows,
    )
    return DataFrame(_jdf, SQLContext.getOrCreate(spark.sparkContext))


def table_to_df(
    spark: SparkSession, client: str, table: str, col_fs: str = "",
    batch: bool = False, start_row: str = "", end_row: str = ""
) -> DataFrame:
    """Load Spark DataFrame from Accumulo table.

    :param spark: Spark session
    :param client: Accumulo client properties filepath
    :param table: Accumulo table name
    :param col_fs: Comma separated column family values to read
    :param batch: Group ranges into batches or not. Helps to reduce overhead
                  when querying a large number of small ranges.
    :param start_row: Use start range of row id if set (inclusive)
    :param end_row: Use end range of row id if set (inclusive)
    :return: DataFrame
    """
    scala_utils = spark.sparkContext._jvm.com.microsoft.ai.accumulo.ScalaUtils

    _jdf = scala_utils.tableToDf(
        spark._jsparkSession,
        client,
        table,
        col_fs,
        batch,
        start_row,
        end_row
    )
    return DataFrame(_jdf, SQLContext.getOrCreate(spark.sparkContext))


def df_to_table(
    spark: SparkSession, df: DataFrame, client: str, table: str,
    num_parts: int = 8, splits: str = "", write_mode: str = "batch", replace: bool = False,
    num_thread: int = 3, batch_memory: int = int(5e+7),
    core_site: str = "", hdfs_site: str = ""
):
    """Inject Spark DataFrame into Accumulo table.
    DataFrame should include a column named "id" which will be used as the table's row id.

    :param spark: Spark session
    :param df: Spark DataFrame
    :param client: Accumulo client properties filepath
    :param table: Accumulo table name
    :param num_parts: Number of partitions to use. Should be NUM-EXECUTORS * n for the best performance.
    :param splits: Comma separated split points of row ids.
    :param write_mode: Write mode: "batch" or "bulk".
    :param replace: Delete existing table.
    :param num_thread: Number of threads to write data. In batch mode, the number of threads per partition batch-writer.
                       I.e., Total threads in batch mode will be num_parts x num_thread
    :param batch_memory: Batch ingestion mode setting: BatchWriter memory.
    :param core_site: Bulk ingestion mode setting: Hadoop core-site.xml path.
    :param hdfs_site: Bulk ingestion mode setting: Hadoop hdfs-site.xml path.
    """
    scala_utils = spark.sparkContext._jvm.com.microsoft.ai.accumulo.ScalaUtils

    scala_utils.dfToTable(
        spark._jsparkSession,
        df._jdf,
        client,
        table,
        num_parts,
        splits,
        write_mode,
        replace,
        num_thread,
        batch_memory,
        core_site,
        hdfs_site
    )

def compact(
    client: str,
    table: str,
    start_row: str = "",
    end_row: str = "",
    wait: bool = False
):
    """Force trigger major compaction.

    :param client: Accumulo client properties filepath
    :param table: Accumulo table name
    :param start_row: Use start range of row id if set (inclusive)
    :param end_row: Use end range of row id if set (inclusive)
    :param wait: Wait until the compaction done if set True
    """
    scala_utils = spark.sparkContext._jvm.com.microsoft.ai.accumulo.ScalaUtils
    scala_utils.compact(client, table, start_row, end_row, wait)
