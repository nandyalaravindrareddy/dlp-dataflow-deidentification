package com.google.swarm.tokenization.ravisamples;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// beam-playground:
//   name: TestDlp
//   description: An example that counts words in Shakespeare's works.
//   multifile: false
//   pipeline_options: --output output.txt
//   context_line: 204
//   categories:
//     - Combiners
//     - Options
//     - Quickstart
//   complexity: MEDIUM
//   tags:
//     - count
//     - strings

import com.github.wnameless.json.unflattener.JsonUnflattener;
import com.google.api.client.util.Maps;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.*;
import com.google.swarm.tokenization.common.ByteValueConverter;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroGenericCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static com.google.swarm.tokenization.common.JsonConvertor.convertJsonToAvro;


public class TestDlp {

    static String schemaString = "{\n" +
            "  \"namespace\": \"io.sqooba\",\n" +
            "  \"name\": \"user\",\n" +
            "  \"type\": \"record\",\n" +
            "  \"fields\": [\n" +
            "    {\"name\": \"id\", \"type\": \"int\"},\n" +
            "    {\"name\": \"name\", \"type\": \"string\"},\n" +
            "    {\"name\": \"email\", \"type\": \"string\"}\n" +
            "  ]\n" +
            "}";
    static Schema schema = new Schema.Parser().parse(schemaString);

    static Pipeline innerPipe = Pipeline.create();
    PCollectionList pCollectionList = PCollectionList.empty(innerPipe);

    static class ExtractTableGenericRecordFn extends DoFn<Table, PCollection<GenericRecord>> {

        @ProcessElement
        public void processElement(@Element Table inputTable, OutputReceiver<PCollection<GenericRecord>> receiver) {

            List<FieldId> headers = inputTable.getHeadersList();
            List<Table.Row> rows = inputTable.getRowsList();

            PCollection<GenericRecord> grList = innerPipe.apply(Create.of(rows)).apply(ParDo.of(new ExtractGenericRecordFn(headers)))
                    .setCoder(AvroGenericCoder.of(schema));
            //PCollectionList<GenericRecord> grs = P
            grList.apply(
                    "WriteAVRO",
                    AvroIO.writeGenericRecords(schema)
                            .withSuffix(".avro")
                            .to("/Users/mahankali/Desktop/SafeRoom/data")
                            .withCodec(CodecFactory.snappyCodec()));
            receiver.output(grList);
        }
    }

    static class ExtractGenericRecordFn extends DoFn<Table.Row, GenericRecord> {

        private List<FieldId> headers;

        public List<FieldId> getHeaders() {
            return headers;
        }

        public void setHeaders(List<FieldId> headers) {
            this.headers = headers;
        }

        ExtractGenericRecordFn(List<FieldId> headers) {
            this.headers = headers;
        }

        ExtractGenericRecordFn(){

        }
        @ProcessElement
        public void processElement(@Element Table.Row tableRow, OutputReceiver<GenericRecord> receiver) {

            //List<Table.Row> records = inputTable.getRowsList();
            Map<String, Value> flatRecords = new LinkedHashMap<>();
            Map<String, Object> jsonValueMap = Maps.newHashMap();

            //records.forEach(rowToMap);
            IntStream.range(0,headers.size())
                    .forEach(index -> flatRecords.put(headers.get(index).getName(), tableRow.getValues(index)));

            String schemaString = "{\n" +
                    "  \"namespace\": \"io.sqooba\",\n" +
                    "  \"name\": \"user\",\n" +
                    "  \"type\": \"record\",\n" +
                    "  \"fields\": [\n" +
                    "    {\"name\": \"id\", \"type\": \"int\"},\n" +
                    "    {\"name\": \"name\", \"type\": \"string\"},\n" +
                    "    {\"name\": \"email\", \"type\": \"string\"}\n" +
                    "  ]\n" +
                    "}";
            Schema schema = new Schema.Parser().parse(schemaString);
            for (Map.Entry<String, Value> entry : flatRecords.entrySet()) {
                ValueProcessor valueProcessor = new ValueProcessor(entry);
                jsonValueMap.put(valueProcessor.cleanKey(), valueProcessor.convertedValue());
            }
            String unflattenedRecordJson = new JsonUnflattener(jsonValueMap).unflatten();
            receiver.output(convertJsonToAvro(schema, unflattenedRecordJson));
        }
    }

    public static class ValueProcessor {

        /** REGEX pattern to extract a string value's actual type that is suffixed with the key name. */
        private static final Pattern VALUE_PATTERN = Pattern.compile("/(?<type>\\w+)$");

        private final String rawKey;
        private final Value value;

        ValueProcessor(Map.Entry<String, Value> entry) {
            this.rawKey = entry.getKey();
            this.value = entry.getValue();
        }

        /**
         * Converts a {@link Value} object to Schema appropriate Java object.
         *
         * @return Java Object equivalent for the Value Object.
         */
        Object convertedValue() {
            String keyType = keyType();

            switch (value.getTypeCase()) {
                case INTEGER_VALUE:
                    return new BigInteger(String.valueOf(value.getIntegerValue()));
                case FLOAT_VALUE:
                    return new BigDecimal(String.valueOf(value.getFloatValue()));

                case BOOLEAN_VALUE:
                    return value.getBooleanValue();

                case TYPE_NOT_SET:
                    return null;
                default:
                case STRING_VALUE:
                    if (keyType != null && keyType.equals("bytes")) {
                        return ByteValueConverter.of(value).asJsonString();
                    }
                    return value.getStringValue();
            }
        }

        /** Returns the original type of a string value. */
        private String keyType() {
            java.util.regex.Matcher matcher = VALUE_PATTERN.matcher(rawKey);
            if (matcher.find()) {
                return matcher.group("type");
            }

            return null;
        }

        /** Remove the type-suffix from string value's key. */
        String cleanKey() {
            return VALUE_PATTERN.matcher(rawKey).replaceFirst("").replaceFirst("^\\$\\.", "");
        }
    }
    // [END extract_words_fn]

    public static class GenerateGenericRecord
            extends PTransform<PCollection<Table>, PCollection<PCollection<GenericRecord>>> {

        @Override
        public PCollection<PCollection<GenericRecord>> expand(PCollection<Table> tables) {

            PCollection<PCollection<GenericRecord>> pGR = tables.apply(ParDo.of(new ExtractTableGenericRecordFn()));
            // pGR.setCoder(AvroGenericCoder.of(schema));
            return pGR;
        }
    }

    /**
     * Options supported by {@link TestDlp}.
     *
     * <p>Concept #4: Defining your own configuration options. Here, you can add your own arguments to
     * be processed by the command-line parser, and specify default values for them. You can then
     * access the options values in your pipeline code.
     *
     * <p>Inherits standard configuration options.
     */
    // [START TestDlp_options]
    public interface TestDlpOptions extends PipelineOptions {

        /**
         * By default, this example reads from a public dataset containing the text of King Lear. Set
         * this option to choose a different input file or glob.
         */
        @Description("Path of the file to read from")
        @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
        String getInputFile();

        void setInputFile(String value);

        /** Set this required option to specify where to write the output. */
        @Description("Path of the file to write to")
        @Required
        String getOutput();

        void setOutput(String value);
    }
    // [END TestDlp_options]

    static void runTestDlp() throws IOException {
        Pipeline p = Pipeline.create();

        String projectId = "avid-booth-387215";
        Table table = deIdentifyTableRowSuppress(projectId, tableCre());

        java.util.List<com.google.privacy.dlp.v2.Table.Row> lsRows = table.getRowsList();
        java.util.List<com.google.privacy.dlp.v2.FieldId> headerList = table.getHeadersList();

        PCollection<Table.Row> collectionRows = p.apply(Create.of(lsRows));

        String schemaString = "{\n" +
                "  \"namespace\": \"io.sqooba\",\n" +
                "  \"name\": \"user\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"id\", \"type\": \"int\"},\n" +
                "    {\"name\": \"name\", \"type\": \"string\"},\n" +
                "    {\"name\": \"email\", \"type\": \"string\"}\n" +
                "  ]\n" +
                "}";
        Schema schema = new Schema.Parser().parse(schemaString);
        p.getCoderRegistry().registerCoderForClass(GenericRecord.class, AvroGenericCoder.of(schema));
        PCollection<GenericRecord> pCollectionGenericRecords =
                collectionRows.apply("Construct GenericRecord",
                        ParDo.of(new ExtractGenericRecordFn(headerList)));

        pCollectionGenericRecords.apply(
                "WriteAVRO",
                AvroIO.writeGenericRecords(schema)
                        .withSuffix(".avro")
                        .to("/home/india/saferoom/op/")
                        .withCodec(CodecFactory.snappyCodec()).withNumShards(1));

        p.run().waitUntilFinish();
    }


    public static Table tableCre() {
        Table tableToDeIdentify =
                Table.newBuilder()
                        .addHeaders(FieldId.newBuilder().setName("id").build())
                        .addHeaders(FieldId.newBuilder().setName("name").build())
                        .addHeaders(FieldId.newBuilder().setName("email").build())
                        .addRows(
                                Table.Row.newBuilder()
                                        .addValues(Value.newBuilder().setIntegerValue(101).build())
                                        .addValues(Value.newBuilder().setStringValue("Charles Dickens").build())
                                        .addValues(Value.newBuilder().setStringValue("1afdk@gak.com").build())
                                        .build())
                        .addRows(
                                Table.Row.newBuilder()
                                        .addValues(Value.newBuilder().setIntegerValue(22).build())
                                        .addValues(Value.newBuilder().setStringValue("Jane Austen").build())
                                        .addValues(Value.newBuilder().setStringValue("2afdk@gak.com").build())
                                        .build())
                        .addRows(
                                Table.Row.newBuilder()
                                        .addValues(Value.newBuilder().setIntegerValue(55).build())
                                        .addValues(Value.newBuilder().setStringValue("Mark Twain").build())
                                        .addValues(Value.newBuilder().setStringValue("3afdk@gak.com").build())
                                        .build())
                        .build();
        return tableToDeIdentify;
    }


    public static Table deIdentifyTableRowSuppress(String projectId, Table tableToDeIdentify)
            throws IOException {
        // Initialize client that will be used to send requests. This client only needs to be created
        // once, and can be reused for multiple requests. After completing all of your requests, call
        // the "close" method on the client to safely clean up any remaining background resources.
        try (DlpServiceClient dlp = DlpServiceClient.create()) {
            // Specify what content you want the service to de-identify.
            ContentItem contentItem = ContentItem.newBuilder().setTable(tableToDeIdentify).build();

            // Specify when the content should be de-identified.
            RecordCondition.Condition condition =
                    RecordCondition.Condition.newBuilder()
                            .setField(FieldId.newBuilder().setName("AGE").build())
                            .setOperator(RelationalOperator.GREATER_THAN)
                            .setValue(Value.newBuilder().setIntegerValue(89).build())
                            .build();
            // Apply the condition to record suppression.
            RecordSuppression recordSuppressions =
                    RecordSuppression.newBuilder()
                            .setCondition(
                                    RecordCondition.newBuilder()
                                            .setExpressions(
                                                    RecordCondition.Expressions.newBuilder()
                                                            .setConditions(
                                                                    RecordCondition.Conditions.newBuilder().addConditions(condition).build())
                                                            .build())
                                            .build())
                            .build();
            // Use record suppression as the only transformation
            RecordTransformations transformations =
                    RecordTransformations.newBuilder().addRecordSuppressions(recordSuppressions).build();

            DeidentifyConfig deidentifyConfig =
                    DeidentifyConfig.newBuilder().setRecordTransformations(transformations).build();

            // Combine configurations into a request for the service.
            DeidentifyContentRequest request =
                    DeidentifyContentRequest.newBuilder()
                            .setParent(LocationName.of(projectId, "global").toString())
                            .setItem(contentItem)
                            .setDeidentifyConfig(deidentifyConfig)
                            .build();

            // Send the request and receive response from the service.
            DeidentifyContentResponse response = dlp.deidentifyContent(request);

            System.out.println("Table after de-identification: " + response.getItem().getTable());

            return response.getItem().getTable();
        }

    }

    public static void main(String[] args) throws IOException {

        runTestDlp();
    }
}