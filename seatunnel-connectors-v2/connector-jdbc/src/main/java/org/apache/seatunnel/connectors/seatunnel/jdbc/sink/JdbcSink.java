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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PostFailException;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@AutoService(SeaTunnelSink.class)
public class JdbcSink
    implements SeaTunnelSink<SeaTunnelRow, JdbcSinkState, XidInfo, JdbcAggregatedCommitInfo> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSink.class);

    private Config pluginConfig;

    private SeaTunnelRowType seaTunnelRowType;

    private JobContext jobContext;

    private JdbcSinkOptions jdbcSinkOptions;

    private JdbcDialect dialect;

    private JdbcConnectionProvider connectionProvider;

    private List<String> preSqls;

    private List<String> postSqls;

    @Override
    public String getPluginName() {
        return "Jdbc";
    }

    @Override
    public void prepare(Config pluginConfig)
        throws PrepareFailException {
        this.pluginConfig = pluginConfig;
        this.jdbcSinkOptions = new JdbcSinkOptions(this.pluginConfig);
        this.dialect = JdbcDialectLoader.load(jdbcSinkOptions.getJdbcConnectionOptions().getUrl());
        this.connectionProvider = new SimpleJdbcConnectionProvider(jdbcSinkOptions.getJdbcConnectionOptions());
        this.preSqls = jdbcSinkOptions.getJdbcConnectionOptions().getPrepareSql();
        this.postSqls = jdbcSinkOptions.getJdbcConnectionOptions().getPostSql();
        if (this.preSqls != null && this.preSqls.size() > 0) {
            LOG.info("Execute prepare sqls: {}", this.preSqls);
            try {
                execute(preSqls);
            } catch (SQLException | ClassNotFoundException ex) {
                throw new PrepareFailException(getPluginName(), PluginType.SINK, ex.getMessage());
            }
        }
    }

    @Override
    public void post(Config pluginConfig)
        throws PostFailException {
        if (this.postSqls != null && this.postSqls.size() > 0) {
            LOG.info("Execute post sqls: {}", this.postSqls);
            try {
                execute(postSqls);
            } catch (SQLException | ClassNotFoundException ex) {
                throw new PostFailException(getPluginName(), PluginType.SINK, ex.getMessage());
            }
        }
    }

    @Override
    public SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> createWriter(SinkWriter.Context context)
        throws IOException {
        SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> sinkWriter;
        if (jdbcSinkOptions.isExactlyOnce()) {
            sinkWriter = new JdbcExactlyOnceSinkWriter(
                context,
                jobContext,
                dialect,
                jdbcSinkOptions,
                seaTunnelRowType,
                new ArrayList<>()
            );
        } else {
            sinkWriter = new JdbcSinkWriter(
                context,
                dialect,
                jdbcSinkOptions,
                seaTunnelRowType);
        }

        return sinkWriter;
    }

    @Override
    public SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> restoreWriter(SinkWriter.Context context, List<JdbcSinkState> states)
        throws IOException {
        if (jdbcSinkOptions.isExactlyOnce()) {
            return new JdbcExactlyOnceSinkWriter(
                context,
                jobContext,
                dialect,
                jdbcSinkOptions,
                seaTunnelRowType,
                states
            );
        }
        return SeaTunnelSink.super.restoreWriter(context, states);
    }

    @Override
    public Optional<SinkAggregatedCommitter<XidInfo, JdbcAggregatedCommitInfo>> createAggregatedCommitter() {
        if (jdbcSinkOptions.isExactlyOnce()) {
            return Optional.of(new JdbcSinkAggregatedCommitter(jdbcSinkOptions));
        }
        return Optional.empty();
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
        if (this.jdbcSinkOptions.getJdbcConnectionOptions().isShardTable()) {
            String shardColumn = jdbcSinkOptions.getJdbcConnectionOptions().getShardColumn();
            int shardColumnIndex;
            try {
                shardColumnIndex = seaTunnelRowType.indexOf(shardColumn);
            } catch (RuntimeException ex) {
                throw new JdbcConnectorException(JdbcConnectorErrorCode.NO_SUITABLE_SHARD_COLUMN, ex.getMessage());
            }
            Class clazz = seaTunnelRowType.getFieldType(shardColumnIndex).getTypeClass();
            if (!(Objects.equals(clazz, Long.class) || Objects.equals(clazz, Integer.class)
                    || Objects.equals(clazz, Short.class))) {
                throw new JdbcConnectorException(JdbcConnectorErrorCode.SHOULD_SHARD_COLUMN_WITH_NUMBER_TYPE,
                        "shard column class=" + clazz.getName());
            }
        }
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return this.seaTunnelRowType;
    }

    @Override
    public Optional<Serializer<JdbcAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        if (jdbcSinkOptions.isExactlyOnce()) {
            return Optional.of(new DefaultSerializer<>());
        }
        return Optional.empty();

    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public Optional<Serializer<XidInfo>> getCommitInfoSerializer() {
        if (jdbcSinkOptions.isExactlyOnce()) {
            return Optional.of(new DefaultSerializer<>());
        }
        return Optional.empty();
    }

    private void execute(List<String> sqls) throws SQLException, ClassNotFoundException {
        try (Statement stmt = this.connectionProvider.getOrEstablishConnection().createStatement()) {
            for (String sql : sqls) {
                stmt.execute(sql);
            }
        } finally {
            this.connectionProvider.closeConnection();
        }
    }
}
