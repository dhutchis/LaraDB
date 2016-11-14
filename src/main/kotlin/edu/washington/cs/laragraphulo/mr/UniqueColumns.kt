package edu.washington.cs.laragraphulo.mr

import com.beust.jcommander.Parameter
import org.apache.accumulo.core.cli.MapReduceClientOnRequiredTable
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Value
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner
import java.io.IOException
import java.util.*


/**
 * See https://accumulo.apache.org/1.8/examples/mapred
 *
 * ```
 $ACCUMULO_HOME/bin/tool.sh target/lara-graphulo-1.0-SNAPSHOT-all.jar edu.washington.cs.laragraphulo.mr.UniqueColumns -i accumulo-1.8 -z localhost:2181  --output /dhutchis/uc -t public_adhoc_netflow -u root -p secret --reducers 1
 * ```
 *
 * ```
$ bin/hdfs dfs -cat /dhutchis/uc/part-r-00000
cf:
cq:Dir
cq:Dport
cq:DstAddr
cq:Dur
cq:Label
cq:Proto
cq:RATE
cq:Sport
cq:SrcAddr
cq:SrcBytes
cq:State
cq:TotPkts
cq:dTos
cq:sTos
 * ```
 */
class UniqueColumns : Configured(), Tool {

  class UMapper : Mapper<Key, Value, Text, Text>() {
    private val temp = Text()

    @Throws(IOException::class, InterruptedException::class)
    public override fun map(key: Key, value: Value, context: Mapper<Key, Value, Text, Text>.Context) {
      temp.set(CF)
      val cf = key.columnFamilyData
      temp.append(cf.backingArray, cf.offset(), cf.length())
      context.write(temp, EMPTY)

      temp.set(CQ)
      val cq = key.columnQualifierData
      temp.append(cq.backingArray, cq.offset(), cq.length())
      context.write(temp, EMPTY)
    }

    companion object {
      private val CF = Text("cf:")
      private val CQ = Text("cq:")
    }
  }

  class UReducer : Reducer<Text, Text, Text, Text>() {
    @Throws(IOException::class, InterruptedException::class)
    public override fun reduce(key: Text, values: Iterable<Text>, context: Reducer<Text, Text, Text, Text>.Context) {
      context.write(key, EMPTY)
    }
  }

  private class Opts : MapReduceClientOnRequiredTable() {
    @Parameter(names = arrayOf("--output"), description = "output directory")
    var output: String? = null
    @Parameter(names = arrayOf("--reducers"), description = "number of reducers to use", required = true)
    var reducers: Int = 0
    @Parameter(names = arrayOf("--offline"), description = "run against an offline table")
    var offline = false
  }

  @Throws(Exception::class)
  override fun run(args: Array<String>): Int {
    val opts = Opts()
    opts.parseArgs(UniqueColumns::class.java.name, args)

    val jobName = this.javaClass.simpleName + "_" + System.currentTimeMillis()

    val job = Job.getInstance(conf)
    job.jobName = jobName
    job.setJarByClass(this.javaClass)

    var clone = opts.tableName
    var conn: Connector? = null

    opts.setAccumuloConfigs(job)

    if (opts.offline) {
      /*
       * this example clones the table and takes it offline. If you plan to run map reduce jobs over a table many times, it may be more efficient to compact the
       * table, clone it, and then keep using the same clone as input for map reduce.
       */

      conn = opts.connector
      clone = opts.tableName + "_" + jobName
      conn!!.tableOperations().clone(opts.tableName, clone, true, HashMap<String, String>(), HashSet<String>())
      conn.tableOperations().offline(clone)

      AccumuloInputFormat.setOfflineTableScan(job, true)
      AccumuloInputFormat.setInputTableName(job, clone)
    }

    job.setInputFormatClass(AccumuloInputFormat::class.java)

    job.setMapperClass(UMapper::class.java)
    job.mapOutputKeyClass = Text::class.java
    job.mapOutputValueClass = Text::class.java

    job.setCombinerClass(UReducer::class.java)
    job.setReducerClass(UReducer::class.java)

    job.numReduceTasks = opts.reducers

    job.setOutputFormatClass(TextOutputFormat::class.java)
    TextOutputFormat.setOutputPath(job, Path(opts.output!!))

    job.waitForCompletion(true)

    if (opts.offline) {
      conn!!.tableOperations().delete(clone)
    }

    return if (job.isSuccessful) 0 else 1
  }

  companion object {
    private val EMPTY = Text()

    @JvmStatic
    @Throws(Exception::class)
    fun main(args: Array<String>) {
      val res = ToolRunner.run(Configuration(), UniqueColumns(), args)
      System.exit(res)
    }
  }

}