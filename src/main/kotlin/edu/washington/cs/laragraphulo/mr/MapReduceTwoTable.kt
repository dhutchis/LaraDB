package edu.washington.cs.laragraphulo.mr

import com.beust.jcommander.Parameter
import org.apache.accumulo.core.cli.MapReduceClientOpts
import org.apache.accumulo.core.client.AccumuloSecurityException
import org.apache.accumulo.core.client.mapreduce.AccumuloMultiTableInputFormat
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat
import org.apache.accumulo.core.client.mapreduce.InputTableConfig
import org.apache.hadoop.mapreduce.Job

open class MapReduceTwoTable : MapReduceClientOpts() {

  @Parameter(names = arrayOf("-t1", "--table1"), required = true, description = "table 1")
  val table1: String? = null

  @Parameter(names = arrayOf("-t2", "--table2"), required = true, description = "table 2")
  val table2: String? = null

  @Parameter(names = arrayOf("-o", "--outputTable"), required = true, description = "output table")
  val tableOut: String? = null

  @Parameter(names = arrayOf("-tf", "--tokenFile"), description = "File in hdfs containing the user's authentication token; create with \"bin/accumulo create-token\"")
  private val tokenFile = ""

  @Throws(AccumuloSecurityException::class)
  override fun setAccumuloConfigs(job: Job) {
    super.setAccumuloConfigs(job)

    val principal = principal

    if (tokenFile.isEmpty()) {
      val token = token
      AccumuloMultiTableInputFormat.setConnectorInfo(job, principal, token)
      AccumuloOutputFormat.setConnectorInfo(job, principal, token)
    } else {
      AccumuloMultiTableInputFormat.setConnectorInfo(job, principal, tokenFile)
      AccumuloOutputFormat.setConnectorInfo(job, principal, tokenFile)
    }
    val c1 = InputTableConfig()
    val c2 = InputTableConfig()

    val configs = mapOf(table1 to c1, table2 to c2)
    AccumuloMultiTableInputFormat.setInputTableConfigs(job, configs)
    AccumuloMultiTableInputFormat.setScanAuthorizations(job, auths)
    AccumuloOutputFormat.setCreateTables(job, true)
    AccumuloOutputFormat.setDefaultTableName(job, tableOut)
  }
}
