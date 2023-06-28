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
package com.google.swarm.tokenization.common;

import com.github.wnameless.json.unflattener.JsonUnflattener;
import com.google.api.client.util.Maps;
import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.privacy.dlp.v2.*;
import com.google.swarm.tokenization.beam.DLPDeidentifyText;
import com.google.swarm.tokenization.beam.DLPInspectText;
import com.google.swarm.tokenization.beam.DLPReidentifyText;
import com.google.swarm.tokenization.common.Util.DLPMethod;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;

import com.google.swarm.tokenization.ravisamples.TestDlp;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.swarm.tokenization.common.JsonConvertor.convertJsonToAvro;

@AutoValue
public abstract class DLPTransform
    extends PTransform<PCollection<KV<String, Table.Row>>, PCollectionTuple> {

  public static final Logger LOG = LoggerFactory.getLogger(DLPTransform.class);

  @Nullable
  public abstract String schema();

  public abstract DeidentifyConfig deidConfig();

  public abstract DeidentifyConfig reidConfig();

  @Nullable
  public abstract String inspectTemplateName();

  @Nullable
  public abstract String deidTemplateName();

  public abstract Integer batchSize();

  public abstract String projectId();

  public abstract Character columnDelimiter();

  public abstract DLPMethod dlpmethod();

  public abstract String jobName();

  public abstract PCollectionView<Map<String, List<String>>> headers();

  public abstract Integer dlpApiRetryCount();

  public abstract Integer initialBackoff();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setSchema(String schema);

    public abstract Builder setDeidConfig(DeidentifyConfig deidConfig);

    public abstract Builder setReidConfig(DeidentifyConfig reidConfig);

    public abstract Builder setInspectTemplateName(String inspectTemplateName);

    public abstract Builder setDeidTemplateName(String inspectTemplateName);

    public abstract Builder setBatchSize(Integer batchSize);

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setHeaders(PCollectionView<Map<String, List<String>>> headers);

    public abstract Builder setColumnDelimiter(Character columnDelimiter);

    public abstract Builder setDlpmethod(DLPMethod method);

    public abstract Builder setJobName(String jobName);

    public abstract Builder setDlpApiRetryCount(Integer dlpApiRetryCount);

    public abstract Builder setInitialBackoff(Integer initialBackoff);

    public abstract DLPTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_DLPTransform.Builder();
  }

  @Override
  public PCollectionTuple expand(PCollection<KV<String, Table.Row>> input) {
    switch (dlpmethod()) {
      case INSPECT:
        {
          return input
              .apply(
                  "InspectTransform",
                  DLPInspectText.newBuilder()
                      .setBatchSizeBytes(batchSize())
                      .setColumnDelimiter(columnDelimiter())
                      .setHeaderColumns(headers())
                      .setInspectTemplateName(inspectTemplateName())
                      .setProjectId(projectId())
                      .setDlpApiRetryCount(dlpApiRetryCount())
                      .setInitialBackoff(initialBackoff())
                      .build())
              .apply(
                  "ConvertInspectResponse",
                  ParDo.of(new ConvertInspectResponse(jobName()))
                      .withOutputTags(
                          Util.inspectOrDeidSuccess, TupleTagList.of(Util.inspectOrDeidFailure)));
        }

      case DEID:
        {
          return input
              .apply(
                  "DeIdTransform",
                  DLPDeidentifyText.newBuilder()
                          .setDeidentifyConfig(deidConfig())
                      .setBatchSizeBytes(batchSize())
                      .setColumnDelimiter(columnDelimiter())
                      .setHeaderColumns(headers())
                      .setInspectTemplateName(inspectTemplateName())
                      .setDeidentifyTemplateName(deidTemplateName())
                      .setProjectId(projectId())
                      .setDlpApiRetryCount(dlpApiRetryCount())
                      .setInitialBackoff(initialBackoff())
                      .build())
              .apply(
                  "ConvertDeidResponse",
                  ParDo.of(new ConvertDeidResponse(schema(),headers())).withSideInputs(headers())
                      .withOutputTags(
                          Util.inspectOrDeidSuccess, TupleTagList.of(Util.inspectOrDeidFailure).and(Util.deidGenericRecords)));
        }
      case REID:
        {
          return input
              .apply(
                  "ReIdTransform",
                  DLPReidentifyText.newBuilder()
                          .setReidentifyConfig(reidConfig())
                      .setBatchSizeBytes(batchSize())
                      .setColumnDelimiter(columnDelimiter())
                      .setHeaderColumns(headers())
                      .setInspectTemplateName(inspectTemplateName())
                      .setReidentifyTemplateName(deidTemplateName())
                      .setProjectId(projectId())
                      .setDlpApiRetryCount(dlpApiRetryCount())
                      .setInitialBackoff(initialBackoff())
                      .build())
              .apply(
                  "ConvertReidResponse",
                  ParDo.of(new ConvertReidResponse())
                      .withOutputTags(Util.reidSuccess, TupleTagList.of(Util.reidFailure)));
        }
      default:
        {
          throw new IllegalArgumentException("Please validate DLPMethod param!");
        }
    }
  }

  static class ConvertReidResponse
      extends DoFn<KV<String, ReidentifyContentResponse>, KV<String, TableRow>> {

    private final Counter numberOfBytesReidentified =
        Metrics.counter(ConvertDeidResponse.class, "NumberOfBytesReidentified");

    @ProcessElement
    public void processElement(
        @Element KV<String, ReidentifyContentResponse> element, MultiOutputReceiver out) {

      String deidTableName = BigQueryHelpers.parseTableSpec(element.getKey()).getTableId();
      String tableName = String.format("%s_%s", deidTableName, Util.BQ_REID_TABLE_EXT);
      LOG.info("Table Ref {}", tableName);
      Table originalData = element.getValue().getItem().getTable();
      numberOfBytesReidentified.inc(originalData.toByteArray().length);
      List<String> headers =
          originalData.getHeadersList().stream()
              .map(fid -> fid.getName())
              .collect(Collectors.toList());
      List<Table.Row> outputRows = originalData.getRowsList();
      if (outputRows.size() > 0) {
        for (Table.Row outputRow : outputRows) {
          if (outputRow.getValuesCount() != headers.size()) {
            throw new IllegalArgumentException(
                "BigQuery column count must exactly match with data element count");
          }
          out.get(Util.reidSuccess)
              .output(
                  KV.of(
                      tableName,
                      Util.createBqRow(outputRow, headers.toArray(new String[headers.size()]))));
        }
      }
    }
  }

  static class ConvertDeidResponse
      extends DoFn<KV<String, DeidentifyContentResponse>, KV<String, TableRow>> {

    private String schema;

    private PCollectionView<Map<String, List<String>>> headerCoumnsView;

    public ConvertDeidResponse(String schema,PCollectionView<Map<String, List<String>>> headerCoumnsView) {
      this.schema = schema;
      this.headerCoumnsView = headerCoumnsView;
    }
    private final Counter numberOfRowDeidentified =
        Metrics.counter(ConvertDeidResponse.class, "numberOfRowDeidentified");

    @ProcessElement
    public void processElement(ProcessContext processContext,MultiOutputReceiver out) {

      KV<String, DeidentifyContentResponse> element=processContext.element();
      Map<String, List<String>> headerColumnMap = processContext.sideInput(headerCoumnsView);
      String fileName = element.getKey();
      Table tokenizedData = element.getValue().getItem().getTable();
      LOG.info("Table de-identified returned with {} rows", tokenizedData.getRowsCount());
      numberOfRowDeidentified.inc(tokenizedData.getRowsCount());
      List<String> headers = headerColumnMap.get(fileName);
      /*List<String> headers =
          tokenizedData.getHeadersList().stream()
              .map(fid -> fid.getName())
              .collect(Collectors.toList());*/
      List<Table.Row> outputRows = tokenizedData.getRowsList();
      if (outputRows.size() > 0) {
        for (Table.Row outputRow : outputRows) {
          if (outputRow.getValuesCount() != headers.size()) {
            throw new IllegalArgumentException(
                "CSV file's header count must exactly match with data element count");
          }
          Schema schemaObject =  new Schema.Parser().parse(ReadGcsObject.getGcsObjectContent(schema));
          GenericRecord genericRecord = generateGenericRecord(outputRow,headers,schemaObject);
          out.get(Util.deidGenericRecords).output(genericRecord);
          out.get(Util.inspectOrDeidSuccess)
              .output(
                  KV.of(
                      fileName,
                      Util.createBqRow(outputRow, headers.toArray(new String[headers.size()]))));
        }
      }
    }
  }

  static class ConvertInspectResponse
      extends DoFn<KV<String, InspectContentResponse>, KV<String, TableRow>> {

    private String jobName;

    public ConvertInspectResponse(String jobName) {
      this.jobName = jobName;
    }

    // Counter to track total number of Inspection Findings fetched from DLP Inspection response
    private final Counter numberOfInspectionFindings =
        Metrics.counter(ConvertInspectResponse.class, "numberOfInspectionFindings");

    // Counter to track total number of times Inspection Findings got truncated in the
    // in the DLP Inspection response
    private final Counter numberOfTimesFindingsTruncated =
        Metrics.counter(ConvertInspectResponse.class, "numberOfTimesFindingsTruncated");

    // Counter to track total number of times Inspection Findings generated in the
    // this should be same number as number of total DLP API calls
    private final Counter numberOfTimesFindingsGenerated =
        Metrics.counter(ConvertInspectResponse.class, "numberOfTimesFindingsGenerated");

    @ProcessElement
    public void processElement(
        @Element KV<String, InspectContentResponse> element, MultiOutputReceiver out) {
      String fileName = element.getKey().split("\\~")[0];
      String timeStamp = Util.getTimeStamp();

      if (element.getValue().getResult().getFindingsTruncated()) {
        numberOfTimesFindingsTruncated.inc();
      }

      numberOfTimesFindingsGenerated.inc();

      element
          .getValue()
          .getResult()
          .getFindingsList()
          .forEach(
              finding -> {
                Row row =
                    Row.withSchema(Util.dlpInspectionSchema)
                        .addValues(
                            jobName,
                            fileName,
                            timeStamp,
                            finding.getQuote(),
                            finding.getInfoType().getName(),
                            finding.getLikelihood().name(),
                            finding.getLocation().getCodepointRange().getStart(),
                            finding.getLocation().getCodepointRange().getEnd(),
                            finding
                                .getLocation()
                                .getContentLocationsList()
                                .get(0)
                                .getRecordLocation()
                                .getFieldId()
                                .getName())
                        .build();
                numberOfInspectionFindings.inc();
                out.get(Util.inspectOrDeidSuccess)
                    .output(KV.of(Util.BQ_DLP_INSPECT_TABLE_NAME, Util.toTableRow(row)));
              });
      element
          .getValue()
          .findInitializationErrors()
          .forEach(
              error -> {
                out.get(Util.inspectOrDeidFailure)
                    .output(
                        KV.of(
                            Util.BQ_ERROR_TABLE_NAME,
                            Util.toTableRow(
                                Row.withSchema(Util.errorSchema)
                                    .addValues(fileName, timeStamp, error.toString(), null)
                                    .build())));
              });
    }
  }

  static GenericRecord generateGenericRecord(Table.Row tableRow,List<String> headers,Schema schema){
    Map<String, Value> flatRecords = new LinkedHashMap<>();
    Map<String, Object> jsonValueMap = Maps.newHashMap();

    IntStream.range(0,headers.size())
            .forEach(index -> flatRecords.put(headers.get(index), tableRow.getValues(index)));

    for (Map.Entry<String, Value> entry : flatRecords.entrySet()) {
      ValueProcessor valueProcessor = new ValueProcessor(entry);
      jsonValueMap.put(valueProcessor.cleanKey(), valueProcessor.convertedValue());
    }
    String unattendedRecordJson = new JsonUnflattener(jsonValueMap).unflatten();
    return convertJsonToAvro(schema, unattendedRecordJson);
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


}
