package com.google.swarm.tokenization.ravisamples;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RnnD {
    public static void main(String[] args) throws IOException {
        // Define the Avro schema with the decimal logical type
        Schema schema = createAvroSchema();

        // Create a decimal value
        String decimalString = "2000.56";
        BigDecimal decimalValue = new BigDecimal(decimalString);

        // Convert decimal to Avro bytes
        GenericRecord record = new GenericData.Record(schema);
        record.put("decimalValue", new Conversions.DecimalConversion().toBytes(decimalValue,
                schema,schema.getField("decimalValue").schema().getLogicalType()));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        GenericData.get().createDatumWriter(schema).write(record, encoder);
        encoder.flush();

        byte[] avroBytes = outputStream.toByteArray();

        System.out.println("Decimal String: " + decimalString);
        System.out.println("Avro Bytes: " + bytesToHex(avroBytes));
    }

    private static Schema createAvroSchema() {
        // Define the decimal logical type schema
        Schema decimalSchema = LogicalTypes.decimal(8, 2).addToSchema(Schema.create(Schema.Type.BYTES));

        // Define the Avro record schema
        Schema recordSchema = Schema.createRecord("DecimalRecord", "A record with decimal value", "namespace", false);
        Schema.Field field = new Schema.Field("decimalValue", decimalSchema, null, null);
        List<Schema.Field> ls = new ArrayList<>();
        ls.add(field);
        recordSchema.setFields(ls);

        return recordSchema;
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
