package com.google.swarm.tokenization.ravisamples;
import com.github.wnameless.json.unflattener.JsonUnflattener;
import com.google.api.client.util.Maps;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.*;
import com.google.swarm.tokenization.DLPTextToBigQueryStreamingV2;
import com.google.swarm.tokenization.common.ByteValueConverter;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static com.google.swarm.tokenization.common.JsonConvertor.convertJsonToAvro;


@SuppressWarnings({"unchecked","deprecation"})
public class TestDlp {

    public static final Logger LOG = LoggerFactory.getLogger(TestDlp.class);
    static class ExtractGenericRecordFn extends DoFn<Table.Row, GenericRecord> {

        private List<FieldId> headers;
        String schemaStr;

        ExtractGenericRecordFn(List<FieldId> headers,String schemaStr) {
            this.headers = headers;
            this.schemaStr = schemaStr;
        }

        @ProcessElement
        public void processElement(@Element Table.Row tableRow, OutputReceiver<GenericRecord> receiver) {

            Map<String, Value> flatRecords = new LinkedHashMap<>();
            Map<String, Object> jsonValueMap = Maps.newHashMap();

            IntStream.range(0,headers.size())
                    .forEach(index -> flatRecords.put(headers.get(index).getName(), tableRow.getValues(index)));

            for (Map.Entry<String, Value> entry : flatRecords.entrySet()) {
                ValueProcessor valueProcessor = new ValueProcessor(entry);
                jsonValueMap.put(valueProcessor.cleanKey(), valueProcessor.convertedValue());
            }
            Schema schema = new Schema.Parser().parse(schemaStr);
            String unattendedRecordJson = new JsonUnflattener(jsonValueMap).unflatten();
            receiver.output(convertJsonToAvro(schema, unattendedRecordJson));
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

    @SuppressWarnings("deprecation")
    static void runTestDlp() throws IOException {
        Pipeline p = Pipeline.create();

        String projectId = "avid-booth-387215";
        Table table = deIdentifyTableRowSuppress(projectId, generateDlpTable());

        List<Table.Row> lsRows = table.getRowsList();
        List<FieldId> headerList = table.getHeadersList();

        PCollection<Table.Row> collectionRows = p.apply(Create.of(lsRows));

        String schemaString = "{\n" +
                "  \"namespace\": \"RaviAvroIoDemo\",\n" +
                "  \"name\": \"user\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"id\", \"type\": \"int\"},\n" +
                "    {\"name\": \"name\", \"type\": \"string\"},\n" +
                "    {\"name\": \"email\", \"type\": \"string\"}\n" +
                "  ]\n" +
                "}";
        Schema schema = new Schema.Parser().parse(schemaString);
        PCollection<GenericRecord> pCollectionGenericRecords =
                collectionRows.apply("Construct GenericRecord",
                        ParDo.of(new ExtractGenericRecordFn(headerList,schemaString)))
                                .setCoder(AvroCoder.of(GenericRecord.class, schema));

        pCollectionGenericRecords.apply(
                "WriteAVRO",
                AvroIO.writeGenericRecords(schema)
                        .to("/home/india/saferoom/op/")
                        .withCodec(CodecFactory.snappyCodec())
                        .withSuffix("_ravi_op.avro")
                        .withOutputFilenames()
                        .withNumShards(1));
        p.run().waitUntilFinish();
    }


    public static Table generateDlpTable() {
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