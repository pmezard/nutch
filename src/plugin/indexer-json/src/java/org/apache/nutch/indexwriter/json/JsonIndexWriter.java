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
package org.apache.nutch.indexwriter.json;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.IndexWriterParams;
import org.apache.nutch.indexer.IndexingJob;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchField;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.io.SerializedString;

/**
 * Write Nutch documents to a JSONL file. All fields are serialized by default.
 *
 */
public class JsonIndexWriter implements IndexWriter {

  public static final String JSON_OUTPATH = "outpath";
  public static final Logger LOG = LoggerFactory
      .getLogger(JsonIndexWriter.class);

  private String filename = "nutch.jsonl";
  private Configuration config;

  /** output path / directory */
  private String outputPath = "jsonindexwriter";
  private FileSystem fs;
  protected FSDataOutputStream out;
  private Path jsonLocalOutFile;
  private JsonGenerator jsonGen;

  @Override
  public void open(Configuration conf, String name) throws IOException {
    filename = name;
  }

  /**
   * Initializes the internal variables from a given index writer configuration.
   *
   * @param parameters Params from the index writer configuration.
   * @throws IOException Some exception thrown by writer.
   */
  @Override
  public void open(IndexWriterParams parameters) throws IOException {
    fs = FileSystem.get(config);
    outputPath = parameters.get(JsonIndexWriter.JSON_OUTPATH, outputPath);
    LOG.info("Writing output to {}", outputPath);
    Path outputDir = new Path(outputPath);
    fs = outputDir.getFileSystem(config);
    jsonLocalOutFile = new Path(outputDir, filename);
    if (!fs.exists(outputDir)) {
      fs.mkdirs(outputDir);
    }
    if (fs.exists(jsonLocalOutFile)) {
      // clean-up
      LOG.warn("Removing existing output path {}", jsonLocalOutFile);
      fs.delete(jsonLocalOutFile, true);
    }
    out = fs.create(jsonLocalOutFile);
    JsonFactory jsonFactory = new JsonFactory();
    jsonGen = jsonFactory.createGenerator((DataOutput)out);
    jsonGen.setRootValueSeparator(new SerializedString("\n"));
  }

  @Override
  public void write(NutchDocument doc) throws IOException {
    jsonGen.writeStartObject();
    jsonGen.writeFieldName("fields");
    jsonGen.writeStartObject();
    for (Map.Entry<String, NutchField> field: doc) {
      jsonGen.writeFieldName(field.getKey());
      jsonGen.writeStartArray();
      List<Object> values = field.getValue().getValues();
      for (Object value: field.getValue().getValues()) {
        if (value instanceof Date) {
            jsonGen.writeString(value.toString());
        } else {
            jsonGen.writeString((String)value);
        }
      }
      jsonGen.writeEndArray();
    }
    jsonGen.writeEndObject();
    jsonGen.writeFieldName("weight");
    jsonGen.writeNumber(doc.getWeight());
    jsonGen.writeEndObject();
  }

  /** (deletion of documents is not supported) */
  @Override
  public void delete(String key) {
  }

  @Override
  public void update(NutchDocument doc) throws IOException {
    write(doc);
  }

  @Override
  public void close() throws IOException {
    jsonGen.close();
    out.close();
    LOG.info("Finished JSON index in {}", jsonLocalOutFile);
  }

  /** (nothing to commit) */
  @Override
  public void commit() {
  }

  @Override
  public Configuration getConf() {
    return config;
  }

  @Override
  public void setConf(Configuration conf) {
    config = conf;
  }

  /**
   * Returns {@link Map} with the specific parameters the IndexWriter instance can take.
   *
   * @return The values of each row. It must have the form (KEY, (DESCRIPTION,VALUE)).
   */
  @Override
  public Map<String, Map.Entry<String, Object>> describe() {
    Map<String, Map.Entry<String, Object>> properties = new LinkedHashMap<>();

    properties.put(JsonIndexWriter.JSON_OUTPATH, new AbstractMap.SimpleEntry<>(
        "Output path / directory, default: jsonindexwriter. ",
        this.outputPath));

    return properties;
  }
}
