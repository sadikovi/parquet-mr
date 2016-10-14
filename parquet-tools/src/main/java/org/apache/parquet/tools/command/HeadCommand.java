/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.tools.command;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.tools.Main;
import org.apache.parquet.tools.json.JsonRecordFormatter;
import org.apache.parquet.tools.read.SimpleReadSupport;
import org.apache.parquet.tools.read.SimpleRecord;

public class HeadCommand extends ArgsOnlyCommand {
  private static final long DEFAULT = 5;

  public static final String[] USAGE = new String[] {
    "<input>",
    "where <input> is the parquet file to print to stdout"
  };

  public static final Options OPTIONS;
  static {
    OPTIONS = new Options();
    Option recordsOpt = OptionBuilder.withLongOpt("records")
                               .withDescription("The number of records to show (default: " + DEFAULT + ")")
                               .hasOptionalArg()
                               .create('n');
    Option jsonOpt = OptionBuilder.withLongOpt("json")
                               .withDescription("Show records in JSON format.")
                               .create('j');
    OPTIONS.addOption(recordsOpt);
    OPTIONS.addOption(jsonOpt);
  }

  public HeadCommand() {
    super(1, 1);
  }

  @Override
  public Options getOptions() {
    return OPTIONS;
  }

  @Override
  public String[] getUsageDescription() {
    return USAGE;
  }

  private PathFilter getPartitionFilesFilter() {
    return new PathFilter() {
      @Override
      public boolean accept(Path path) {
        // ignore Spark '_SUCCESS' files
        return !path.getName().equals("_SUCCESS");
      }
    };
  }

  @Override
  public void execute(CommandLine options) throws Exception {
    super.execute(options);

    long num = DEFAULT;
    if (options.hasOption('n')) {
      num = Long.parseLong(options.getOptionValue('n'));
    }

    String[] args = options.getArgs();
    String input = args[0];

    ParquetReader<SimpleRecord> reader = null;
    boolean asJson = options.hasOption('j');
    try {
      PrintWriter writer = new PrintWriter(Main.out, true);
      reader = ParquetReader.builder(new SimpleReadSupport(), new Path(input)).build();
      // we read footer only when output as json
      JsonRecordFormatter.JsonGroupFormatter formatter = null;
      if (asJson) {
        // if input file is a directory search for any metadata
        Configuration conf = new Configuration();
        Path basePath = new Path(input);
        FileSystem fs = basePath.getFileSystem(conf);
        List<Footer> footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(
          conf, Arrays.asList(fs.listStatus(basePath, getPartitionFilesFilter())));
        // select first metadata that we found, fail if none available
        if (footers == null || footers.isEmpty()) {
          throw new IllegalArgumentException("Could not find metadata in " + basePath);
        } else {
          // footer.get(0) returns ParquetMetadata
          ParquetMetadata metadata = footers.get(0).getParquetMetadata();
          formatter = JsonRecordFormatter.fromSchema(metadata.getFileMetaData().getSchema());
        }
      }

      for (SimpleRecord value = reader.read(); value != null && num-- > 0; value = reader.read()) {
        if (asJson) {
          writer.write(formatter.formatRecord(value));
        } else {
          value.prettyPrint(writer);
        }
        writer.println();
      }
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (Exception ex) {
        }
      }
    }
  }
}
