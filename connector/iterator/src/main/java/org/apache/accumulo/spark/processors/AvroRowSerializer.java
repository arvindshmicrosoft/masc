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

package org.apache.accumulo.spark.processors;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.accumulo.spark.record.AvroFastRecord;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

public class AvroRowSerializer {
  // avro writer infra
  private ByteArrayOutputStream binaryBuffer = new ByteArrayOutputStream();
  private DatumWriter<IndexedRecord> writer;
  private BinaryEncoder encoder;

  private AvroFastRecord finalRecord;
  private int[] sourceIndicies;
  private String prunedColumns;

  public AvroRowSerializer(Schema schema, String prunedColumns) {
    this.prunedColumns = prunedColumns;

    // check if we need to prune the output
    if (prunedColumns != null) {
      HashSet<String> columns = new HashSet<String>(Arrays.asList(prunedColumns.split(",")));

      // quick check if the field list is the same, otherwise just output everything
      if (schema.getFields().size() != columns.size()) {
        List<Field> fieldList = schema.getFields().stream().filter(f -> columns.contains(f.name()))
            .map(f -> new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal()))
            // create the list
            .collect(Collectors.toList());

        Schema prunedSchema = Schema.createRecord(fieldList);
        this.finalRecord = new AvroFastRecord(prunedSchema);

        // initialize source to target mapping
        this.sourceIndicies = new int[fieldList.size()];
        for (Field field : prunedSchema.getFields())
          this.sourceIndicies[field.pos()] = schema.getField(field.name()).pos();

        schema = prunedSchema;
      }
    }

    this.writer = new SpecificDatumWriter<>(schema);
    this.encoder = EncoderFactory.get().binaryEncoder(binaryBuffer, null);
  }

  public String getPrunedColumns() {
    return prunedColumns;
  }

  public byte[] serialize(IndexedRecord record) throws IOException {
    // make sure we're at the beginning again
    this.binaryBuffer.reset();

    // copying to final output schema
    if (this.sourceIndicies != null) {
      for (int i = 0; i < this.sourceIndicies.length; i++)
        this.finalRecord.put(i, record.get(this.sourceIndicies[i]));

      record = this.finalRecord;
    }
    // serialize the record
    this.writer.write(record, encoder);

    this.encoder.flush();
    this.binaryBuffer.flush();

    return this.binaryBuffer.toByteArray();
  }
}
