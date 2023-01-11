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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

@Slf4j
public class ShardBufferedBatchStatementExecutor implements JdbcBatchStatementExecutor<SeaTunnelRow> {
    @NonNull
    private final Map<Integer, JdbcBatchStatementExecutor<SeaTunnelRow>> shardStatementExecutors;
    @NonNull
    private final Function<SeaTunnelRow, SeaTunnelRow> valueTransform;
    @NonNull
    private final Integer shardModNumber;

    private final List<SeaTunnelRow> buffer = new ArrayList<>();
    private final Map<Integer, Long> shardRows = new HashMap<>();
    private int shardColumnIndex;

    public ShardBufferedBatchStatementExecutor(Map<Integer, JdbcBatchStatementExecutor<SeaTunnelRow>> shardStatementExecutors,
                                               Function<SeaTunnelRow, SeaTunnelRow> valueTransform,
                                               Integer shardModNumber,
                                               String shardColumn,
                                               SeaTunnelRowType rowType) {
        this.shardStatementExecutors = shardStatementExecutors;
        this.valueTransform = valueTransform;
        this.shardModNumber = shardModNumber;
        this.shardColumnIndex = rowType.indexOf(shardColumn);
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        for (Map.Entry<Integer, JdbcBatchStatementExecutor<SeaTunnelRow>> entry : shardStatementExecutors.entrySet()) {
            entry.getValue().prepareStatements(connection);
        }
    }

    @Override
    public void addToBatch(SeaTunnelRow record) throws SQLException {
        buffer.add(valueTransform.apply(record));
    }

    @Override
    public void executeBatch() throws SQLException {
        if (!buffer.isEmpty()) {
            for (SeaTunnelRow row : buffer) {
                Object data = row.getField(shardColumnIndex);
                if (data instanceof Number) {
                    Number value = (Number) data;
                    int mod = (int) value.longValue() % shardModNumber;
                    shardStatementExecutors.get(mod).addToBatch(row);
                    shardRows.compute(mod, (id, count) -> count == null ? 1 : ++count);
                } else {
                    throw new JdbcConnectorException(JdbcConnectorErrorCode.SHOULD_SHARD_COLUMN_WITH_NUMBER_TYPE,
                        "shard column class=" + data.getClass().getName());
                }
            }
            for (Integer mod : shardRows.keySet()) {
                shardStatementExecutors.get(mod).executeBatch();
            }
            if (log.isDebugEnabled()) {
                for (Map.Entry<Integer, Long> entry : shardRows.entrySet()) {
                    log.debug("Batch size per executor: {}:{}", entry.getKey(), entry.getValue());
                }
            }
            shardRows.clear();
            buffer.clear();
        }
    }

    @Override
    public void closeStatements() throws SQLException {
        if (!buffer.isEmpty()) {
            executeBatch();
        }
        for (Map.Entry<Integer, JdbcBatchStatementExecutor<SeaTunnelRow>> entry : shardStatementExecutors.entrySet()) {
            entry.getValue().closeStatements();
        }
    }
}
