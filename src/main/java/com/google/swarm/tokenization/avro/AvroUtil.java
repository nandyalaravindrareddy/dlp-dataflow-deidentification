/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.swarm.tokenization.avro;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Consumer;

import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import com.google.protobuf.Timestamp;
import com.google.swarm.tokenization.common.ByteValueConverter;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.io.FileIO;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.lang3.StringUtils.isBlank;

/** Various helpers for working with Avro files. */
public class AvroUtil {

  public static final Logger LOG = LoggerFactory.getLogger(AvroUtil.class);

  /**
   * Returns the list of field names from the given schema. Calls itself recursively to flatten
   * nested fields.
   */
  public static void flattenFieldNames(Schema schema, List<String> fieldNames,List<String> deIdentifiedFields, String prefix) {
    for (Schema.Field field : schema.getFields()) {
      if (field.schema().getType() == Schema.Type.RECORD) {
        flattenFieldNamesForAvroFile(field.schema(), fieldNames, deIdentifiedFields,prefix + field.name() + ".");
      } else {
        fieldNames.add(prefix + field.name());
      }
    }
  }

  public static void flattenFieldNamesForAvroFile(Schema schema, List<String> fieldNames,List<String> deIdentifiedFields, String prefix) {
    for (Schema.Field field : schema.getFields()) {
      if (field.schema().getType() == Schema.Type.RECORD) {
        flattenFieldNamesForAvroFile(field.schema(), fieldNames, deIdentifiedFields,prefix + field.name() + ".");
      } else if(field.schema().getType() == Schema.Type.UNION) {
        if(deIdentifiedFields.contains(field.name())){
          fieldNames.add(field.name()+".string");
        }else{
          fieldNames.add(field.name()+"."+field.schema().getTypes().get(1).getType().getName());
        }

      }else{
        fieldNames.add(prefix + field.name());
      }
    }
  }

  /** Converts the given Avro value to a type that can be processed by DLP */
  public static Object convertForDLP(Object value, LogicalType logicalType) {
    if (logicalType != null) {
      if (logicalType instanceof LogicalTypes.Date) {
        Instant instant = Instant.EPOCH.plus(Duration.standardDays((int) value));
        return DateTimeFormat.forPattern("yyyy-MM-dd").print(instant);
      } else if (logicalType instanceof LogicalTypes.TimestampMillis) {
        Instant instant = new Instant((int) value);
        return DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss").print(instant);
      } else if (logicalType instanceof LogicalTypes.TimestampMicros) {
        Instant instant = new Instant((int) value / 1000);
        return DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss").print(instant);
      }
    }
    return value;
  }

  /** Substitute for a `null` value used in the Avro value flattening function below. */
  private static class NullValue {}

  /**
   * Traverses the given Avro record's (potentially nested) schema and sends all flattened values to
   * the given consumer. Uses a stack to perform an iterative, preorder traversal of the schema
   * tree.
   */
  public static void getFlattenedValues(Table.Row.Builder rowBuilder,GenericRecord rootNode,List<String> deIdentifiedFields) {
    Deque<Object> stack = new ArrayDeque<>();
    // Start with the given record
    stack.push(rootNode);
    while (!stack.isEmpty()) {
      Object node = stack.pop();
      if (node instanceof GenericRecord) {
        // The current node is a record, so we go one level deeper...
        GenericRecord record = (GenericRecord) node;
        List<Schema.Field> fields = record.getSchema().getFields();
        ListIterator<Schema.Field> iterator = fields.listIterator();
        // Push all of the record's sub-fields to the stack in reverse order
        // to prioritize sub-fields from left to right in subsequent loop iterations.
        while (iterator.hasNext()) {
          Schema.Field field = iterator.next();
          Schema schema = field.schema();
          String fieldName = field.name();
          Object value = record.get(fieldName);

          Schema.Type type = schema.getType();
          LogicalType logicalType = schema.getLogicalType();
          if(schema.getType().getName().equalsIgnoreCase("union")){
            type = schema.getTypes().get(1).getType();
            logicalType = schema.getTypes().get(1).getLogicalType();
          }
          if(value==null){
            rowBuilder.addValues(Value.getDefaultInstance()).build();
          } else if (logicalType!=null){
            convertAvroLogicalFieldValueToDLPValue(rowBuilder,value,logicalType);
          }else {
            if(deIdentifiedFields.contains(fieldName) && type!=Schema.Type.BYTES)
              type = Schema.Type.STRING;
            constructRowBuilder(rowBuilder, value, type);
          }

          /*if (value == null) {
            // Special case: A stack can't accept a null value,
            // so we substitute for a mock object instead.
            //stack.push(new NullValue());
            rowBuilder.addValues(Value.newBuilder().setStringValue(new NullValue).build());
          } else {
            Object convertedValue = AvroUtil.convertForDLP(value, logicalType);

            rowBuilder.addValues(Value.newBuilder().setStringValue("").build());
          }*/


        }
      }
    }
  }

  public static void constructRowBuilder(Table.Row.Builder rowBuilder,Object value,Schema.Type type){
    switch (type) {
      case STRING:
          rowBuilder.addValues(Value.newBuilder().setStringValue(value.toString()).build());
        break;

      case BOOLEAN:
          rowBuilder.addValues(Value.newBuilder().setBooleanValue((boolean) value).build());
        break;

      case FLOAT:
          rowBuilder.addValues(Value.newBuilder().setFloatValue((float) value).build());
        break;

      case DOUBLE:
          rowBuilder.addValues(Value.newBuilder().setFloatValue((double) value).build());
        break;

      case INT:
          rowBuilder.addValues(Value.newBuilder().setIntegerValue((int) value).build());
        break;

      case LONG:
          rowBuilder.addValues(Value.newBuilder().setIntegerValue((long) value).build());
        break;

      case FIXED:
        rowBuilder.addValues(ByteValueConverter.convertBytesToValue(((GenericFixed) value).bytes()));
        break;

      case BYTES:
        rowBuilder.addValues(ByteValueConverter.convertBytesToValue(((ByteBuffer) value).array()));
        break;

      case NULL:
        break;

      case MAP:
        throw new IllegalArgumentException(String.format("Unsupported Type MAP "));
    }
  }


  /** Byte channel that enables random access for Avro files. */
  public static class AvroSeekableByteChannel implements SeekableInput {

    private final SeekableByteChannel channel;

    public AvroSeekableByteChannel(SeekableByteChannel channel) {
      this.channel = channel;
    }

    @Override
    public void seek(long p) throws IOException {
      channel.position(p);
    }

    @Override
    public long tell() throws IOException {
      return channel.position();
    }

    @Override
    public long length() throws IOException {
      return channel.size();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      ByteBuffer buffer = ByteBuffer.wrap(b, off, len);
      return channel.read(buffer);
    }

    @Override
    public void close() throws IOException {
      channel.close();
    }
  }

  /** Converts a ReadableFile to a byte channel enabled for random access. */
  public static AvroSeekableByteChannel getChannel(FileIO.ReadableFile file) {
    SeekableByteChannel channel;
    try {
      channel = file.openSeekable();
    } catch (IOException e) {
      LOG.error("Failed to open file {}", e.getMessage());
      throw new RuntimeException(e);
    }
    return new AvroSeekableByteChannel(channel);
  }

  public static void convertAvroLogicalFieldValueToDLPValue(Table.Row.Builder rowBuilder,Object fieldValue, LogicalType logicalType) {
    if (logicalType instanceof LogicalTypes.Date) {
      int daysSinceEpoch = (int) fieldValue;
      LocalDate date = LocalDate.ofEpochDay(daysSinceEpoch);
      String dateString = date.toString();
      rowBuilder.addValues(Value.newBuilder().setStringValue(dateString).build());
    } else if (logicalType instanceof LogicalTypes.TimestampMillis) {
      long milliseconds = (long) fieldValue;
      java.time.Instant instant = java.time.Instant.ofEpochMilli(milliseconds);
      Timestamp timestamp = Timestamp.newBuilder()
              .setSeconds(instant.getEpochSecond())
              .setNanos(instant.getNano())
              .build();
      rowBuilder.addValues(Value.newBuilder().setTimestampValue(timestamp).build());
    } else if (logicalType instanceof LogicalTypes.Decimal) {
      ByteBuffer byteBuffer = (ByteBuffer) fieldValue;
      int precision = ((LogicalTypes.Decimal) logicalType).getPrecision();
      int scale = ((LogicalTypes.Decimal) logicalType).getScale();
      byte[] decimalBytes = byteBuffer.array();
      BigInteger unscaledValue = new BigInteger(decimalBytes);
      BigDecimal decimalValue = new BigDecimal(unscaledValue, scale);
      rowBuilder.addValues(Value.newBuilder().setStringValue(decimalValue.toString()).build());
    }
  }

}


