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
package com.google.swarm.tokenization;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.privacy.dlp.v2.DeidentifyConfig;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.FieldTransformation;
import com.google.privacy.dlp.v2.Table;
import com.google.swarm.tokenization.avro.AvroReaderSplittableDoFn;
import com.google.swarm.tokenization.avro.ConvertAvroRecordToDlpRowDoFn;
import com.google.swarm.tokenization.avro.GenericRecordCoder;
import com.google.swarm.tokenization.beam.ConvertCSVRecordToDLPRow;
import com.google.swarm.tokenization.common.*;
import com.google.swarm.tokenization.json.ConvertJsonRecordToDLPRow;
import com.google.swarm.tokenization.json.JsonReaderSplitDoFn;
import com.google.swarm.tokenization.txt.ConvertTxtToDLPRow;
import com.google.swarm.tokenization.txt.ParseTextLogDoFn;
import com.google.swarm.tokenization.txt.TxtReaderSplitDoFn;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.repackaged.core.org.apache.commons.compress.utils.FileNameUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DLPTextToBigQueryStreamingV2 {

  public static final Logger LOG = LoggerFactory.getLogger(DLPTextToBigQueryStreamingV2.class);
  private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(3);
  private static final Duration WINDOW_INTERVAL = Duration.standardSeconds(3);
  /** PubSub configuration for default batch size in number of messages */
  public static final Integer PUB_SUB_BATCH_SIZE = 1000;
  /** PubSub configuration for default batch size in bytes */
  public static final Integer PUB_SUB_BATCH_SIZE_BYTES = 10000;

  public static void main(String[] args) {

    DLPTextToBigQueryStreamingV2PipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DLPTextToBigQueryStreamingV2PipelineOptions.class);

    run(options);
  }

  public static PipelineResult run(DLPTextToBigQueryStreamingV2PipelineOptions options) {
    Pipeline p = Pipeline.create(options);

    switch (options.getDLPMethod()) {
      case INSPECT:
      case DEID:
        String fileName = StringUtils.EMPTY;
        try {
          fileName = FileNameUtils.getBaseName(GcsPath.fromUri(options.getFilePattern()).getObject());
          runInspectAndDeidPipeline(p, options,fileName);
        }catch(Exception e){
          LOG.error("DLP De-Identification process is failed : due to "+e.getMessage());
          BigQueryOps.updateRecord(options.getDataset(),options.getTableRef(),"Failed",fileName);
        }
        break;

      case REID:
        runReidPipeline(p, options);
        break;

      default:
        throw new IllegalArgumentException("Please validate DLPMethod param!");
    }

    return p.run();
  }

  private static void runInspectAndDeidPipeline(
      Pipeline p, DLPTextToBigQueryStreamingV2PipelineOptions options,String fileName) {
    LOG.info("DLP de-identification process is started for file {}",fileName);
    //BigQueryOps.updateRecord(options.getDataset(),options.getTableRef(),"InProgress",fileName);
    DeidentifyConfig deidentifyConfig = JsonConvertor.parseJson(ReadGcsObject.getGcsObjectContent(options.getDeidConfig()), DeidentifyConfig.class);
    List<String> deIdentifiedFields = new ArrayList<String>();
    List<FieldTransformation> fields = deidentifyConfig.getRecordTransformations().getFieldTransformationsList();
    for(FieldTransformation fieldTransformation : fields){
      for(FieldId fieldId : fieldTransformation.getFieldsList()){
          deIdentifiedFields.add(fieldId.getName());
      }
    }
    Schema schema = new Schema.Parser().parse(ReadGcsObject.getGcsObjectContent(options.getDeidSchema()));
    PCollection<KV<String, ReadableFile>> inputFiles = p.apply(FileIO.match().filepattern(options.getFilePattern()))
            .apply(FileIO.readMatches())
            .apply(ParDo.of(new DoFn<ReadableFile, KV<String, ReadableFile>>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                ReadableFile readableFile = c.element();
                String fileName = readableFile.getMetadata().resourceId().getFilename();
                c.output(KV.of(fileName,readableFile));
              }
            }));

   /* PCollection<KV<String, ReadableFile>> inputFiles =
        p.apply(
                FilePollingTransform.newBuilder()
                    .setFilePattern(options.getFilePattern())
                    .setInterval(DEFAULT_POLL_INTERVAL)
                    .build())
            .apply("Fixed Window", Window.into(FixedWindows.of(WINDOW_INTERVAL)));*/

    final PCollectionView<Map<String, List<String>>> headers =
        inputFiles.apply(
            "Extract Column Names",
            ExtractColumnNamesTransform.newBuilder()
                .setFileType(options.getFileType())
                .setHeaders(options.getHeaders())
                .setColumnDelimiter(options.getColumnDelimiter())
                    .setDeIdentifiedFields(deIdentifiedFields)
                .build());


    PCollection<KV<String, Table.Row>> records;

    switch (options.getFileType()) {
      case AVRO:
        records =
            inputFiles
                .apply(
                    ParDo.of(
                        new AvroReaderSplittableDoFn(
                            options.getKeyRange(), options.getSplitSize())))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), GenericRecordCoder.of()))
                .apply(ParDo.of(new ConvertAvroRecordToDlpRowDoFn(deIdentifiedFields)));
        break;
      case TSV:
        options.setColumnDelimiter('\t');
      case CSV:
        records =
            inputFiles
                .apply(
                    "SplitCSVFile",
                    ParDo.of(
                        new CSVFileReaderSplitDoFn(
                            options.getRecordDelimiter(), options.getSplitSize())))
                .apply(
                    "ConvertToDLPRow",
                    ParDo.of(new ConvertCSVRecordToDLPRow(options.getColumnDelimiter(), headers))
                        .withSideInputs(headers));
        break;
      case JSONL:
        records =
            inputFiles
                .apply(
                    "SplitJSONFile",
                    ParDo.of(
                        new JsonReaderSplitDoFn(
                            options.getKeyRange(),
                            options.getRecordDelimiter(),
                            options.getSplitSize())))
                .apply("ConvertToDLPRow", ParDo.of(new ConvertJsonRecordToDLPRow()));
        break;
      case TXT:
        PCollectionTuple recordTuple =
            inputFiles
                .apply(
                    "SplitTextFile",
                    ParDo.of(
                        new TxtReaderSplitDoFn(
                            options.getKeyRange(),
                            options.getRecordDelimiter(),
                            options.getSplitSize())))
                .apply(
                    "ParseTextFile",
                    ParDo.of(new ParseTextLogDoFn())
                        .withOutputTags(
                            Util.agentTranscriptTuple,
                            TupleTagList.of(Util.customerTranscriptTuple)));

        records =
            PCollectionList.of(recordTuple.get(Util.agentTranscriptTuple))
                .and(recordTuple.get(Util.customerTranscriptTuple))
                .apply("Flatten", Flatten.pCollections())
                .apply(
                    "ConvertToDLPRow",
                    ParDo.of(new ConvertTxtToDLPRow(options.getColumnDelimiter(), headers))
                        .withSideInputs(headers));
        break;
      default:
        throw new IllegalArgumentException("Please validate FileType parameter");
    }

    //Schema schema = new Schema.Parser().parse(schemaString);
    PCollectionTuple pCollectionTuple = records
        .apply(
            "DLPTransform",
            DLPTransform.newBuilder()
                    .setSchema(options.getDeidSchema())
                    .setDeidConfig(deidentifyConfig)
                    .setReidConfig(deidentifyConfig)
                .setBatchSize(options.getBatchSize())
                .setInspectTemplateName(options.getInspectTemplateName())
                .setDeidTemplateName(options.getDeidentifyTemplateName())
                .setDlpmethod(options.getDLPMethod())
                .setProjectId(options.getDLPParent())
                .setHeaders(headers)
                .setColumnDelimiter(options.getColumnDelimiter())
                .setJobName(options.getJobName())
                .setDlpApiRetryCount(options.getDlpApiRetryCount())
                .setInitialBackoff(options.getInitialBackoff())
                .build());


    PCollection<GenericRecord> deidGenericRecords = pCollectionTuple.get(Util.deidGenericRecords).setCoder(AvroCoder.of(GenericRecord.class, schema));
    //TupleTag<KV<String, TableRow>> inspectOrDeidSuccessMap = pCollectionTuple.get(Util.inspectOrDeidSuccess);
    deidGenericRecords.apply(
            "WriteAVRO",
            AvroIO.writeGenericRecords(schema)
                    .to(options.getOutputAvroBucket()+fileName)
                    .withCodec(CodecFactory.snappyCodec())
                    .withNumShards(1));


    /*pCollectionTuple.get(Util.inspectOrDeidSuccess).apply(
            "StreamInsertToBQ",
            BigQueryDynamicWriteTransform.newBuilder()
                    .setDatasetId(options.getDataset())
                    .setProjectId(options.getProject())
                    .build());*/
  }

  private static void runReidPipeline(
      Pipeline p, DLPTextToBigQueryStreamingV2PipelineOptions options) {
    // TODO: there is no reason for this method to key elements by table reference because
    // there is always a single possible reference for this batch pipeline.
    // Changing it will require additional refactoring which is outside of the scope of the current
    // fix.

    DeidentifyConfig reidConfig = JsonConvertor.parseJson(ReadGcsObject.getGcsObjectContent(options.getDeidConfig()), DeidentifyConfig.class);
    Schema schema = new Schema.Parser().parse(ReadGcsObject.getGcsObjectContent(options.getDeidSchema()));

    PCollection<KV<String, TableRow>> records =
        p.apply(
            "ReadFromBQ",
            BigQueryReadTransform.newBuilder()
                .setTableRef(options.getTableRef())
                .setReadMethod(options.getReadMethod())
                .setKeyRange(options.getKeyRange())
                //.setQuery(Util.getQueryFromGcs(options.getQueryPath()))
                    .setQuery("SELECT id,name,email FROM `avid-booth-387215.ravidataset.ravi_inputfiles_employee` LIMIT 10")
                .build());

    PCollectionView<Map<String, List<String>>> selectedColumns =
        records
            .apply("GetARow", Sample.any(1))
            .apply("GetColumns", ParDo.of(new BigQueryTableHeaderDoFn()))
            .apply("CreateSideInput", View.asMap());

    PCollection<KV<String, TableRow>> reidData =
        records
            .apply("ConvertTableRow", ParDo.of(new MergeBigQueryRowToDlpRow()))
            .apply(
                "DLPTransform",
                DLPTransform.newBuilder()
                        .setReidConfig(reidConfig)
                        .setDeidConfig(reidConfig)
                    .setBatchSize(options.getBatchSize())
                    .setInspectTemplateName(options.getInspectTemplateName())
                    .setDeidTemplateName(options.getDeidentifyTemplateName())
                    .setDlpmethod(options.getDLPMethod())
                    .setProjectId(options.getDLPParent())
                    .setHeaders(selectedColumns)
                    .setColumnDelimiter(options.getColumnDelimiter())
                    .setJobName(options.getJobName())
                    .setDlpApiRetryCount(options.getDlpApiRetryCount())
                    .setInitialBackoff(options.getInitialBackoff())
                    .build())
            .get(Util.reidSuccess);

    // BQ insert
    reidData.apply(
        "BigQueryInsert",
        BigQueryDynamicWriteTransform.newBuilder()
            .setDatasetId(options.getDataset())
            .setProjectId(options.getProject())
            .build());
    // pubsub publish
    if (options.getTopic() != null) {
      reidData
          .apply("ConvertToPubSubMessage", ParDo.of(new PubSubMessageConverts()))
          .apply(
              "PublishToPubSub",
              PubsubIO.writeMessages()
                  .withMaxBatchBytesSize(PUB_SUB_BATCH_SIZE_BYTES)
                  .withMaxBatchSize(PUB_SUB_BATCH_SIZE)
                  .to(options.getTopic()));
    }
  }
}
