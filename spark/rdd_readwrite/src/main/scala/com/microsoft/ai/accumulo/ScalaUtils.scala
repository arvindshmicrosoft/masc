// Version 1.0.6
package com.microsoft.ai.accumulo

import org.apache.accumulo.core.client.{
    Accumulo, AccumuloClient, BatchWriter, BatchWriterConfig,
    IteratorSetting, MutationsRejectedException
}
import org.apache.accumulo.core.data.{Key, Mutation, Range, Value}
import org.apache.accumulo.hadoop.mapreduce.{
    AccumuloFileOutputFormat, AccumuloInputFormat
}
import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{expr, first}
import org.apache.spark.sql.types.StringType

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer


object ScalaUtils {
    /** Get names of the Accumulo tables.
     *
     *  @param spark   Spark session
     *  @param client  Accumulo client properties filepath
     *
     *  @return DataFrame
     */
    def tables(spark: SparkSession, client: String): DataFrame = {
        val props = Accumulo.newClientProperties().from(client).build()

        val cl = Accumulo.newClient().from(props).build()
        val tables = cl.tableOperations().list()
        cl.close()

        import spark.implicits._
        tables.toList.toDF
    }

    /** Get first n rows from the table.
     *
     *  @param spark   Spark session
     *  @param client  Accumulo client properties filepath
     *  @param table   Accumulo table name
     *  @param numRows Number of rows to retrieve
     *
     *  @return DataFrame
     */
    def tableHead(spark: SparkSession, client: String, table: String, numRows: Int = 1): DataFrame = {
        val props = Accumulo.newClientProperties().from(client).build()
        val cl = Accumulo.newClient().from(props).build()

        val scan = cl.createScanner(table)
        val it = scan.iterator()
        val l = new ListBuffer[Seq[String]]()
        var i = 0
        while (it.hasNext && i < numRows) {
            val firstEntry = it.next()
            val k = firstEntry.getKey
            val v = firstEntry.getValue
            l += Seq(
                k.getRow().toString,
                k.getColumnFamily().toString,
                k.getColumnQualifier().toString,
                v.toString
            )
            i += 1
        }
        scan.close()
        cl.close()

        import spark.implicits._
        l.map(x => (x.head, x(1), x(2), x(3))).toDF("id", "colF", "colQ", "value")
    }

    /** Load Spark DataFrame from Accumulo table
     *
     *  @param spark    Spark session
     *  @param client   Accumulo client properties filepath
     *  @param table    Accumulo table name
     *  @param colFs    Comma separated column family values to read
     *  @param batch    Group ranges into batches or not.
     *                  Helps to reduce overhead when querying a large number of small ranges.
     *  @param startRow Use start range of row id if set (inclusive)
     *  @param endRow   Use end range of row id if set (inclusive)
     *
     *  @return DataFrame
     */
    def tableToDf(spark: SparkSession, client: String, table: String, colFs: String = "",
                  batch: Boolean = false, startRow: String = "", endRow: String = ""): DataFrame = {
        val props = Accumulo.newClientProperties().from(client).build()

        val cl = Accumulo.newClient().from(props).build()
        if (!cl.tableOperations().exists(table)) {
            throw new IllegalArgumentException("Table %s does not exist.".format(table))
        }
        cl.close()

        val sc = spark.sparkContext
        val job = Job.getInstance()

        var inputFormat = AccumuloInputFormat.configure().clientProperties(props).table(table).batchScan(batch)

        // Set range for scanning if specified
        val ranges = new java.util.ArrayList[Range]()
        val sRow = if (startRow.length > 0) startRow else null
        val eRow = if (endRow.length > 0) endRow else null
        ranges.add(new Range(sRow, eRow))
        inputFormat = inputFormat.ranges(ranges)

        // Set columns for scanning if specified
        if (colFs.length > 0) {
            val columns = new java.util.ArrayList[IteratorSetting.Column]()
            for (name <- colFs.split("\\s*,\\s*").toSeq) {
                columns.add(new IteratorSetting.Column(new Text(name)))
            }
            inputFormat = inputFormat.fetchColumns(columns)
        }

        inputFormat.store(job)

        val rawRDD = sc.newAPIHadoopRDD(
            job.getConfiguration,
            classOf[AccumuloInputFormat],
            classOf[Key],
            classOf[Value]
        )

        import spark.implicits._
        val rawDF = rawRDD.mapPartitions( partition => {
            partition.map({
                case (k, v) => (
                  k.getRow().toString,
                  k.getColumnFamily().toString,  // To get column qualifier, add k.getColumnQualifier().toString
                  v.toString
                )
            })
        }).toDF("id", "colF", "value")

        if (colFs.length > 0) {
            rawDF.groupBy("id").pivot("colF", colFs.split("\\s*,\\s*").toSeq).agg(first($"value"))
        }
        else {
            rawDF.groupBy("id").pivot("colF").agg(first($"value"))
        }
    }


    /** Inject Spark DataFrame into Accumulo table.
     *  DataFrame should include a column named "id" which will be used as the table's row id.
     *
     *  To bulk ingest data from remote, first ssh into the master node and run following commands
     *  to create 'root' directory:
     *  $ hadoop fs -mkdir /user/root
     *  $ hadoop fs -chown root /user/root
     * Alternatively, do programmatically:
     * FsShell fsShell = new FsShell(opts.getHadoopConfig())
     * fsShell.run(new String[] {"-chmod", "-R", "777", workDir})
     * ...
     *
     * Also, copy core-site.xml and hdfs-site.xml from Accumulo cluster and provide the file path to the function.
     *
     *  @param spark         Spark session
     *  @param df            Spark DataFrame
     *  @param client        Accumulo client properties filepath
     *  @param table         Accumulo table name
     *  @param numParts      Number of partitions to use. Should be NUM-EXECUTORS * n for the best performance.
     *  @param splits        Comma separated spliting points of row ids.  If not provided, don't split.
     *  @param writeMode     Write mode: "batch" or "bulk".
     *  @param replace       If true, delete existing table.
     *  @param numThreads    Number of threads to write data. In batch mode, the number of threads per partition batch-writer.
     *                       I.e., Total threads in batch mode will be num_parts x num_thread
     *  @param batchMemory   Batch ingestion mode setting: BatchWriter memory.
     *  @param coreSite      Bulk ingestion mode setting: Hadoop core-site.xml path.
     *  @param hdfsSite      Bulk ingestion mode setting: Hadoop hdfs-site.xml path.
     */
    def dfToTable(spark: SparkSession, df: DataFrame, client: String, table: String,
                  numParts: Int = 8, splits: String = "",
                  writeMode: String = "batch", replace: Boolean = false,
                  numThreads: Int = 3, batchMemory: Long = 5e+7.toLong,
                  coreSite: String = "", hdfsSite: String = ""): Unit = {
        if (!(df.columns contains "id")) {
            throw new IllegalArgumentException("Input DataFrame should have 'id' column.")
        }

        import spark.implicits._
        val sc = spark.sparkContext

        val props = Accumulo.newClientProperties().from(client).build()
        val cl = Accumulo.newClient().from(props).build()
        if (replace && cl.tableOperations().exists(table)) cl.tableOperations().delete(table)
        if (!cl.tableOperations().exists(table)) cl.tableOperations().create(table)

        // Pre-split table
        if (splits.length > 0) {
            val newSplits = new java.util.TreeSet[Text]()
            for (name <- splits.split("\\s*,\\s*").toSeq) newSplits.add(new Text(name))
            cl.tableOperations().addSplits(table, newSplits)
        }
        val listSplits = new java.util.ArrayList[String]()
        for (s <- cl.tableOperations().listSplits(table)) listSplits.add(s.toString)
        cl.close()

        val sortedColFs = df.columns.filter(_ != "id").sorted
        val bcColFs = sc.broadcast(sortedColFs)

        // Convert all columns to String as Accumulo stores byte[] data
        val dfStr = df.schema.filter(_.dataType != StringType).foldLeft(df) {
            case (df, col) => df.withColumn(col.name, df(col.name).cast(StringType))
        }

        if (writeMode == "batch") {
            val dfParted = dfStr.repartition(numParts, $"id")
            dfParted.foreachPartition { partition =>
                // Intentionally created an Accumulo client for each partition to avoid attempting to
                // serialize it and send it to each remote process.
                var cl = None: Option[AccumuloClient]
                var bw = None: Option[BatchWriter]
                try {
                    cl = Some(Accumulo.newClient().from(props).build())
                    bw = Some(cl.get.createBatchWriter(
                        table,
                        new BatchWriterConfig().setMaxWriteThreads(numThreads).setMaxMemory(batchMemory)
                    ))
                    // Don't see clear benefit of using grouped-foreach, so just do foreach.
                    partition.foreach { record =>
                        val m = new Mutation(record.getAs[String]("id"))
                        bcColFs.value.foreach { colF =>
                            // Add .visibility(v).timestamp(t) if needed
                            m.at().family(colF).qualifier("").put(record.getAs[String](colF))
                        }
                        try {
                            bw.get.addMutation(m)
                        }
                        catch {
                            case e: MutationsRejectedException => e.printStackTrace()
                        }
                    }
                }
                finally {
                    if (bw.isDefined) bw.get.close()
                    if (cl.isDefined) cl.get.close()
                }
            }
        }
        else if (writeMode == "bulk") {
            // Bulk ingestion requires sorted R-files
            val dfParted = dfStr.repartitionByRange(numParts, $"id")

            // Unpivot and sort
            val stackAs = new StringBuilder("stack(%s".format(sortedColFs.length))
            sortedColFs.foreach( s =>
                stackAs ++= ", '%s', %s".format(s, s)
            )
            stackAs ++= ") as (colF, value)"
            val dfStacked = dfParted.select($"id", expr(stackAs.toString)).sortWithinPartitions($"id", $"colF")

            // Convert into key-value paired RDD
            val pairedRdd = dfStacked.rdd.mapPartitions( partition => {
                partition.map( row =>
                    (new Key(row.getString(0), row.getString(1), ""), new Value(row.getString(2)))
                )
            })

            // Create HDFS directory for bulk import
            val conf = new Configuration()
            if (coreSite.length > 0) conf.addResource(new Path(coreSite))
            if (hdfsSite.length > 0) conf.addResource(new Path(hdfsSite))

            val hdfs = FileSystem.get(conf)
            val rootPath = hdfs.getHomeDirectory
            if (!hdfs.isDirectory(rootPath)) {
                hdfs.mkdirs(rootPath)
            }

            val outputDir = new Path(rootPath.toString + "/bulk_import")
            if (hdfs.isDirectory(outputDir)) hdfs.delete(outputDir, true)

            // https://static.javadoc.io/org.apache.accumulo/accumulo-hadoop-mapreduce/2.0.0/org/apache/accumulo/hadoop/mapreduce/AccumuloFileOutputFormat.html
            // Other settings e.g. .fileBlockSize(b).compression(type).summarizers(sc1, sc2)
            val job =  Job.getInstance(conf)
            AccumuloFileOutputFormat.configure().outputPath(outputDir).store(job)

            // Write into HDFS.
            pairedRdd.saveAsNewAPIHadoopFile(
                outputDir.toString,
                classOf[Key],
                classOf[Value],
                classOf[AccumuloFileOutputFormat],
                job.getConfiguration
            )

            val cl = Accumulo.newClient().from(props).build()
            cl.tableOperations().importDirectory(outputDir.toString).to(table).threads(numThreads).load()
            cl.close()
        }
        else {
            throw new IllegalArgumentException("Write mode should be either 'batch' or 'bulk'")
        }
    }


    /** Force trigger major compaction
     *
     * @param client    Accumulo client properties filepath
     * @param table     Accumulo table name
     * @param startRow  Use start range of row id if set (inclusive)
     * @param endRow    Use end range of row id if set (inclusive)
     * @param wait      Wait until the compaction done if set true
     */
    def compact(client: String, table: String,
                startRow: String = "", endRow: String = "", wait: Boolean = false): Unit = {
        val props = Accumulo.newClientProperties().from(client).build()
        val cl = Accumulo.newClient().from(props).build()
        cl.tableOperations().compact(
            table,
            if (startRow.length == 0) null else new Text(startRow),
            if (endRow.length == 0) null else new Text(endRow),
            true,  // flush
            wait
        )
    }
}
