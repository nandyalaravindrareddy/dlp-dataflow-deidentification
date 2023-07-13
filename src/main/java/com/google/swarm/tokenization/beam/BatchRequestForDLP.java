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
package com.google.swarm.tokenization.beam;

import com.google.privacy.dlp.v2.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.ShardedKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Batches input rows to reduce number of requests sent to Cloud DLP service. */
public class BatchRequestForDLP
    extends DoFn<KV<ShardedKey<String>, Table.Row>, KV<ShardedKey<String>, Iterable<Table.Row>>> {

  public static final Logger LOG = LoggerFactory.getLogger(BatchRequestForDLP.class);

  private final Counter numberOfDLPRowsBagged =
      Metrics.counter(BatchRequestForDLP.class, "numberOfDLPRowsBagged");
  private final Counter numberOfDLPRowBags =
      Metrics.counter(BatchRequestForDLP.class, "numberOfDLPRowBags");

  private final Integer batchSizeBytes;

  private final Integer maxDlpTableCells;

  private Integer columnsCount;

  private final PCollectionView<Map<String, List<String>>> headerColumns;

  @StateId("elementsBag")
  private final StateSpec<BagState<KV<ShardedKey<String>, Table.Row>>> elementsBag =
      StateSpecs.bag();

  @TimerId("eventTimer")
  private final TimerSpec eventTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  /**
   * Constructs the batching DoFn.
   *
   * @param batchSize Desired batch size in bytes.
   */
  public BatchRequestForDLP(Integer batchSize,Integer maxDlpTableCells,PCollectionView<Map<String, List<String>>> headerColumns) {
    this.batchSizeBytes = batchSize;
    this.maxDlpTableCells = maxDlpTableCells;
    this.headerColumns = headerColumns;
  }

  @ProcessElement
  public void process(ProcessContext context,
      @Element KV<ShardedKey<String>, Table.Row> element,
      @StateId("elementsBag") BagState<KV<ShardedKey<String>, Table.Row>> elementsBag,
      @TimerId("eventTimer") Timer eventTimer,
      BoundedWindow w) {
    if (headerColumns != null) {
      Map<String, List<String>> headerColumnMap = context.sideInput(headerColumns);
      columnsCount = headerColumnMap.values().stream().findAny().get().size();
    }
    element = context.element();
    elementsBag.add(element);
    eventTimer.set(w.maxTimestamp());
  }

  /**
   * Outputs the elements buffered in the elementsBag in batches of desired size.
   *
   * @param elementsBag element buffer.
   * @param output Batched input elements.
   */
  @OnTimer("eventTimer")
  public void onTimer(
      @StateId("elementsBag") BagState<KV<ShardedKey<String>, Table.Row>> elementsBag,
      OutputReceiver<KV<ShardedKey<String>, Iterable<Table.Row>>> output) {
    if (elementsBag.read().iterator().hasNext()) {
      ShardedKey<String> key = elementsBag.read().iterator().next().getKey();
      int bufferSize = 0;
      List<Table.Row> rows = new ArrayList<>();

      for (KV<ShardedKey<String>, Table.Row> element : elementsBag.read()) {
        int elementSize = element.getValue().getSerializedSize();
        boolean clearBuffer = bufferSize + elementSize > batchSizeBytes;
        //boolean isMaxTableValuesReached = rows.size()*columnsCount > maxDlpTableCells;
        if (clearBuffer) {
          LOG.debug("Clear buffer of {} bytes, Key {}", bufferSize, element.getKey());
          numberOfDLPRowsBagged.inc(rows.size());
          numberOfDLPRowBags.inc();
          output.output(KV.of(key, rows));
          rows = new ArrayList<>();
          bufferSize = 0;
        }
        rows.add(element.getValue());
        bufferSize = bufferSize + elementSize;
      }

      if (!rows.isEmpty()) {
        LOG.debug("Outputting remaining {} rows.", rows.size());
        numberOfDLPRowsBagged.inc(rows.size());
        numberOfDLPRowBags.inc();
        output.output(KV.of(key, rows));
      }
    }
  }
}
