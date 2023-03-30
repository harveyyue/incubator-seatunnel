/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hudi.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiConnectorException;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
public class HudiSourceReader implements SourceReader<SeaTunnelRow, HudiSourceSplit> {

    private static final long THREAD_WAIT_TIME = 500L;

    private final ReadStrategy readStrategy;
    private final SourceReader.Context context;
    private final Set<HudiSourceSplit> sourceSplits;

    public HudiSourceReader(ReadStrategy readStrategy, SourceReader.Context context) {
        this.readStrategy = readStrategy;
        this.context = context;
        this.sourceSplits = new HashSet<>();
    }

    @Override
    public void open() {
    }

    @Override
    public void close() {

    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (sourceSplits.isEmpty()) {
            Thread.sleep(THREAD_WAIT_TIME);
            return;
        }
        sourceSplits.forEach(source -> {
            try {
                // The splitId format:
                // hdfs://cluster/hive_warehouse/workdb.db/test_all_type_20221111/dt=2022-11-11/0d05208f-3858-4d07-9e48-d6bc64a63f69-0_0-78-227_20221112083601841.parquet:0+441815
                String filePath = source.getPath().toString();
                log.info("Reading hudi parquet file: {}", filePath);
                readStrategy.read(filePath, output);
            } catch (Exception e) {
                throw new HudiConnectorException(CommonErrorCode.READER_OPERATION_FAILED, e);
            }
        });
        context.signalNoMoreElement();
    }

    @Override
    public List<HudiSourceSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(sourceSplits);
    }

    @Override
    public void addSplits(List<HudiSourceSplit> splits) {
        sourceSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {

    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {

    }
}
