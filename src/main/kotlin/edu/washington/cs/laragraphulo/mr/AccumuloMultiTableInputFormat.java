/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.washington.cs.laragraphulo.mr;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AbstractInputFormat;
import org.apache.accumulo.core.client.mapreduce.InputTableConfig;
import org.apache.accumulo.core.client.mapreduce.RangeInputSplit;
import org.apache.accumulo.core.client.mapreduce.impl.BatchInputSplit;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.htrace.Sampler;
import org.apache.htrace.Span;
import org.apache.htrace.TraceInfo;
import org.apache.htrace.TraceScope;

/**
 * This class allows MapReduce jobs to use multiple Accumulo tables as the source of data. This {@link org.apache.hadoop.mapreduce.InputFormat} provides keys
 * and values of type {@link Key} and {@link Value} to the Map function.
 *
 * The user must specify the following via static configurator methods:
 *
 * <ul>
 * <li>{@link AccumuloMultiTableInputFormat#setConnectorInfo(Job, String, AuthenticationToken)}
 * <li>{@link AccumuloMultiTableInputFormat#setScanAuthorizations(Job, Authorizations)}
 * <li>{@link AccumuloMultiTableInputFormat#setZooKeeperInstance(Job, ClientConfiguration)}
 * <li>{@link AccumuloMultiTableInputFormat#setInputTableConfigs(Job, Map)}
 * </ul>
 *
 * Other static methods are optional.
 */
public class AccumuloMultiTableInputFormat extends AbstractInputFormat<Key,Value> {

  /**
   * Sets the {@link InputTableConfig} objects on the given Hadoop configuration
   *
   * @param job
   *          the Hadoop job instance to be configured
   * @param configs
   *          the table query configs to be set on the configuration.
   * @since 1.6.0
   */
  public static void setInputTableConfigs(Job job, Map<String,InputTableConfig> configs) {
    requireNonNull(configs);
    InputConfigurator.setInputTableConfigs(CLASS, job.getConfiguration(), configs);
  }

  public static final String MASTER_TRACE_ID = "masterTraceId";
  public static final String MASTER_SPAN_ID = "masterSpanId";

  public static void setMasterTraceId(Job job) {
    Span span = org.apache.htrace.Trace.currentSpan();
    if (span != null) {
      long tid = span.getTraceId();
      long sid = span.getSpanId();
      job.getConfiguration().setLong(MASTER_TRACE_ID, tid);
      job.getConfiguration().setLong(MASTER_SPAN_ID, sid);
    }
  }


  private static final byte[] RANGE_STR = "Range".getBytes(StandardCharsets.UTF_8);

  @Override
  public RecordReader<Key,Value> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
    final long masterTraceId = context.getConfiguration().getLong(MASTER_TRACE_ID, -1);
    final long masterSpanId = context.getConfiguration().getLong(MASTER_SPAN_ID, -1);

    log.setLevel(getLogLevel(context));
    return new AbstractRecordReader<Key,Value>() {

      private TraceScope traceScope;

      @Override
      public void initialize(InputSplit inSplit, TaskAttemptContext attempt) throws IOException {
        final String service = AccumuloMultiTableInputFormat.class.getSimpleName();

//        Configuration confBackup = new BaseConfiguration();
//        for (Map.Entry<String, String> entry : context.getConfiguration()) {
//          confBackup.addProperty(entry.getKey(), entry.getValue());
//        }

//        conf.addConfiguration(confBackup);

//        Iterator it = conf.getKeys();
//        while (it.hasNext()) {
//          String k = (String)it.next();
//          conf.get(k)
//        }


        if (masterTraceId != -1) {
          final ClientConfiguration conf = ClientConfiguration.loadDefault();
          System.out.println("initializing masterTraceId "+masterTraceId+" on service "+service); //+" with conf: "+conf.serialize());
          TracerHolder.initialize(null, service, conf);
          org.apache.htrace.Trace.setProcessId(service);

          TraceInfo tinfo = new TraceInfo(masterTraceId, masterSpanId);
          traceScope = org.apache.htrace.Trace.startSpan("MR input initialize", (Sampler<TraceInfo>) Sampler.ALWAYS, tinfo);

          byte[] r;
          if (inSplit instanceof BatchInputSplit) {
            BatchInputSplit bs = (BatchInputSplit)inSplit;
            r = (bs.getTableName()+" "+bs.getRanges()).getBytes(StandardCharsets.UTF_8);
          } else if (inSplit instanceof RangeInputSplit) {
            RangeInputSplit bs = (RangeInputSplit)inSplit;
            r = (bs.getTableName()+" "+bs.getRange()).getBytes(StandardCharsets.UTF_8);
          } else {
            r = "Unknown range".getBytes(StandardCharsets.UTF_8);
          }
          traceScope.getSpan().addKVAnnotation(RANGE_STR, r);
        }

        super.initialize(inSplit, attempt);
      }

      @Override
      public void close() {
        super.close();

        if (traceScope != null) {
          traceScope.close();
        }

      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        if (scannerIterator.hasNext()) {
          ++numKeysRead;
          Map.Entry<Key,Value> entry = scannerIterator.next();
          currentK = currentKey = entry.getKey();
          currentV = entry.getValue();
          if (log.isTraceEnabled())
            log.trace("Processing key/value pair: " + DefaultFormatter.formatEntry(entry, true));
          return true;
        }
        return false;
      }

      @Override
      protected List<IteratorSetting> contextIterators(TaskAttemptContext context, String tableName) {
        return getInputTableConfig(context, tableName).getIterators();
      }
    };
  }
}
