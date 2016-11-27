package edu.washington.cs.laragraphulo.mr

import com.beust.jcommander.Parameter
import edu.washington.cs.laragraphulo.Loggable
import edu.washington.cs.laragraphulo.info
import edu.washington.cs.laragraphulo.logger
import edu.washington.cs.laragraphulo.opt.old.reduceWithDefault
import org.apache.accumulo.core.client.AccumuloException
import org.apache.accumulo.core.client.ClientConfiguration
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat
import org.apache.accumulo.core.client.mapreduce.RangeInputSplit
import org.apache.accumulo.core.conf.AccumuloConfiguration
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.LongCombiner
import org.apache.accumulo.core.iterators.user.SummingCombiner
import org.apache.accumulo.core.trace.Trace
import org.apache.accumulo.tracer.ZooTraceClient
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.WritableComparable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.util.ShutdownHookManager
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner
import org.apache.log4j.Level
import org.slf4j.Logger
import java.io.DataInput
import java.io.DataOutput
import java.io.IOException
import java.util.*
import org.apache.htrace.SpanReceiver


/**
 * See https://accumulo.apache.org/1.8/examples/mapred
 *
 * ```
$ACCUMULO_HOME/bin/tool.sh target/lara-graphulo-1.0-SNAPSHOT-all.jar edu.washington.cs.laragraphulo.mr.MatMulJob -i accumulo-1.8 -z localhost:2181  -t1 test1 -t2 test2 -o testo -u root -p secret --reducers 1
 * ```
 *
 * ```
createtable test1
table test1
insert x '' a 1
insert y '' a 2
insert x '' b 3
createtable test2
table test2
insert x '' r 5
insert y '' r 6
insert y '' s 7
 * ```
 */
class MatMulJob : Configured(), Tool {

/*  data class RowQual(
//      val row: Text,
//      val qual: Text
//  ) : WritableComparable<RowQual> {
//    override fun write(out: DataOutput?) {
//      row.write(out)
//      qual.write(out)
//    }
//
//    override fun readFields(`in`: DataInput?) {
//      row.readFields(`in`)
//      qual.readFields(`in`)
//    }
//
//    override fun compareTo(other: RowQual): Int {
//      return row.compareTo(other.row).let { if (it == 0) it else qual.compareTo(other.qual) }
//    }
//  }
*/
  data class QualValue(
      /** false from the input table that is less, lexicographically; true for the other */
      var tag: String = "",
      var qual: Text = Text(),
      var value: Value = Value()
  ) : WritableComparable<QualValue> {
    constructor(tag: String, key: Key, value: Value) :
        this(tag, key.columnQualifier, value)

    override fun write(out: DataOutput) {
      out.writeUTF(tag)
      qual.write(out)
      value.write(out)
    }

    override fun readFields(`in`: DataInput) {
      tag = `in`.readUTF()
      qual.readFields(`in`)
      value.readFields(`in`)
    }

    override fun compareTo(other: QualValue): Int {
      return qual.compareTo(other.qual).let { if (it == 0) it else value.compareTo(other.value) }
    }
  }


// Almost can use IdentityMapper<Key,Value>, but need to group under row and column qualifier
  class MatMulMapper : Mapper<Key, Value, Text, QualValue>() {
    private val temp = Text()

    @Throws(IOException::class, InterruptedException::class)
    override fun map(key: Key, value: Value, context: Mapper<Key, Value, Text, QualValue>.Context) {
      val split = context.inputSplit as RangeInputSplit
      val tableName = split.tableName

      key.rowData.let { r -> temp.set(r.backingArray, r.offset(), r.length()) }
//      logger.info{"$temp -> ${QualValue(tableName, key, value)}"}
      context.write(temp, QualValue(tableName, key, value))
    }
 }

  class MatMulReducer : Reducer<Text, QualValue, Text, Mutation>() {
    @Throws(IOException::class, InterruptedException::class)
    override fun reduce(key: Text, values0: Iterable<QualValue>, context: Reducer<Text, QualValue, Text, Mutation>.Context) {
      // DEEP copy values so that we can iterate through it twice
      val values = ArrayList<QualValue>()
      for ((tag, qual, value) in values0) {
        values.add(QualValue(tag, Text(qual), Value(value)))
      }

      val quals = values.map { it.tag }.distinct().sorted().reversed() // TODO: the first table may not be the lexicographically lower one - pass information to the reducer and mapper
      require(quals.size <= 2) { "Too many input table tags: $quals" }
//      logger.info{"$key -> values $values"}
//      logger.info{"$key -> quals $quals"}
//      println("$key -> $quals")
      if (quals.size <= 1) return

      val t1 = quals.first()
      val (l1, l2) = values.partition { it.tag == t1 }
//      logger.info{"$key -> l1 $l1 l2 $l2"}

      // initial size guess
      val guess = l2.map {
        1 + // family
            it.qual.length + 1 + // qual
            1 + //vis
            1 + 1 + //hasts, deleted
            it.value.size + 1 //val
      }.reduceWithDefault(0) { i, j -> i + j } + 2 // to be safe

      for ((tag1, qual1, value1) in l1) {
        val v1 = value1.toString().toLong() // TODO: fixed parsing as Long with String encoding
        // TODO: optimization of re-using Mutation objects could be passed onto Graphulo
        val m = Mutation(qual1, guess)
        for ((tag2, qual2, value2) in l2) {

          val v2 = value2.toString().toLong()

//          logger.info{"$qual1, $qual2 -> ${v1*v2}"}
//          if (qual1.toString() == "1" && qual2.toString() == "1009")
//            logger.info{"$key: $qual1, $qual2 -> ${v1*v2}"}

          val v = Value((v1*v2).toString().toByteArray())
          m.put(EMPTY, qual2, v)

        }
        context.write(null, m) // null writes to the default output table
      }
    }
  }

  private class Opts : MapReduceTwoTable() {
//    @Parameter(names = arrayOf("--output"), description = "output directory")
//    var output: String? = null
    @Parameter(names = arrayOf("--reducers"), description = "number of reducers to use", required = true)
    var reducers: Int = 1
//    @Parameter(names = arrayOf("--offline"), description = "run against an offline table")
//    var offline = false
    @Parameter(names = arrayOf("--noDelete"), description = "don't delete the result table")
    var noDelete = false
  }














  @Throws(Exception::class)
  override fun run(args: Array<String>): Int {
    //SpanReceiverHost -- hadoop
//    val sr = ZooTraceClient()
    val service = MatMulJob::class.java.simpleName
    TracerHolder.initialize(null, service, ClientConfiguration.loadDefault())
    org.apache.htrace.Trace.setProcessId(service)
    val opts = Opts()
    opts.parseArgs(MatMulJob::class.java.simpleName, args)
//    DistributedTrace.enable(null, MatMulJob::class.java.name, opts.clientConfiguration)

    val jobName = this.javaClass.simpleName + "_" + System.currentTimeMillis()

    val job = Job.getInstance(conf)
    job.jobName = jobName
    job.setJarByClass(this.javaClass)

//    var clone = opts.tableName
//    var conn: Connector? = null

    opts.setAccumuloConfigs(job)

    // delete output table if it exists, create it, add combiner
    val conn = opts.connector
    if (!opts.noDelete && conn.tableOperations().exists(opts.tableOut))
      conn.tableOperations().delete(opts.tableOut)
    if (!opts.noDelete || !conn.tableOperations().exists(opts.tableOut))
      conn.tableOperations().create(opts.tableOut)

    val itset = IteratorSetting(15, SummingCombiner::class.java)
    LongCombiner.setEncodingType(itset, LongCombiner.Type.STRING)
    LongCombiner.setCombineAllColumns(itset, true)
    try {
      conn.tableOperations().attachIterator(opts.tableOut, itset) // all scopes
    } catch (e: AccumuloException) {
      logger.warn("problem setting SummingCombiner $itset", e)
      if (!opts.noDelete)
        throw e
    }


//    if (opts.offline) {
//      /*
//       * this example clones the table and takes it offline. If you plan to run map reduce jobs over a table many times, it may be more efficient to compact the
//       * table, clone it, and then keep using the same clone as input for map reduce.
//       */
//
//      conn = opts.connector
//      clone = opts.tableName + "_" + jobName
//      conn!!.tableOperations().clone(opts.tableName, clone, true, HashMap<String, String>(), HashSet<String>())
//      conn.tableOperations().offline(clone)
//
//      AccumuloInputFormat.setOfflineTableScan(job, true)
//      AccumuloInputFormat.setInputTableName(job, clone)
//    }


    job.inputFormatClass = AccumuloMultiTableInputFormat::class.java
    AccumuloMultiTableInputFormat.setMasterTraceId(job)

    job.mapperClass = MatMulMapper::class.java
    job.mapOutputKeyClass = Text::class.java
    job.mapOutputValueClass = QualValue::class.java

//    job.setCombinerClass(MatMulReducer::class.java)
    job.reducerClass = MatMulReducer::class.java

    job.numReduceTasks = opts.reducers

    job.outputFormatClass = AccumuloOutputFormat::class.java
    job.outputKeyClass = Text::class.java
    job.outputValueClass = Mutation::class.java
//    AccumuloInputFormat.setLogLevel(job, Level.DEBUG)
    AccumuloInputFormat.setBatchScan(job, true) // evil switch
//    AccumuloOutputFormat.setLogLevel(job, Level.TRACE)

//    var scope: TraceScope? = null;
//    if (opts.trace) {
////      DistributedTrace.enable("MapReduce");
//      scope = Trace.startSpan("MapReduce", Sampler.ALWAYS);
//    }
    println("Tracing:: ${Trace.isTracing()}")



    try {
//      println("load class: ${this.javaClass.classLoader.loadClass("org.apache.accumulo.tracer.ZooTraceClient")}")
      print("Before: "); printMemory()
      val t = System.currentTimeMillis()
      job.waitForCompletion(true)
      println("elapsed: ${(System.currentTimeMillis() - t) / 1000.0} s")
      print("After: "); printMemory()
    } finally {
      if (Trace.isTracing())
        Trace.off()
//      DistributedTrace.disable()
//      if (opts.trace) {
//        scope?.close()
////        DistributedTrace.disable()
//      }
    }

//    if (opts.offline) {
//      conn!!.tableOperations().delete(clone)
//    }

    return if (job.isSuccessful) 0 else 1
  }

  companion object : Loggable {

    private fun printMemory() {
      val rt = Runtime.getRuntime()
      println("Memory: ${rt.totalMemory()/1024/1024 - rt.freeMemory()/1024/1024} used, ${rt.totalMemory()/1024/1024} total, ${rt.maxMemory()/1024/1024} max (MB)")
    }


    override val logger: Logger = logger<MatMulJob>()
    private val EMPTY = Text()
    private val EMPTYVAL = Value()

    /*
    To execute on YARN, add to yarn-site.xml per https://stackoverflow.com/questions/33345262/org-apache-hadoop-yarn-exceptions-invalidauxserviceexception-the-auxservicemap

<configuration>
<property>-
<name>yarn.nodemanager.aux-services</name>
<value>mapreduce_shuffle</value>
</property>
</configuration>

Another trick:
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
     */

    @JvmStatic
    @Throws(Exception::class)
    fun main(args: Array<String>) {
      val conf = Configuration()

//      // this should be like defined in your yarn-site.xml
//      conf.set("yarn.resourcemanager.address", "localhost:8032");
//
//// framework is now "yarn", should be defined like this in mapred-site.xm
//      conf.set("mapreduce.framework.name", "yarn");
//
//// like defined in hdfs-site.xml
//      conf.set("fs.default.name", "hdfs://localhost:9000");

      val res = ToolRunner.run(conf, MatMulJob(), args)
      System.exit(res)
    }

  }

}