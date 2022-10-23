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

package org.apache.seatunnel.connectors.seatunnel.kafka.sink;

import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.ASSIGN_PARTITIONS;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.PARTITION;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.PARTITION_KEY;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.PRODUCER_CONFIG_PREFIX;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.TOPIC;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.TRANSACTION_PREFIX;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.KafkaSemantics;
import org.apache.seatunnel.connectors.seatunnel.kafka.serialize.DefaultSeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.kafka.serialize.SeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaSinkState;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * KafkaSinkWriter is a sink writer that will write {@link SeaTunnelRow} to Kafka.
 */
@Slf4j
public class KafkaSinkWriter implements SinkWriter<SeaTunnelRow, KafkaCommitInfo, KafkaSinkState> {

    private final SinkWriter.Context context;
    private final Config pluginConfig;
    private final Function<SeaTunnelRow, SeaTunnelRow> partitionExtractor;

    private String transactionPrefix;
    private long lastCheckpointId = 0;
    private int partition;
    private String topic;
    private List<String> partitionKeys;

    private final KafkaProduceSender<byte[], byte[]> kafkaProducerSender;
    private final SeaTunnelRowSerializer<byte[], byte[]> seaTunnelRowSerializer;

    private static final int PREFIX_RANGE = 10000;

    // check config
    @Override
    public void write(SeaTunnelRow element) {
        ProducerRecord<byte[], byte[]> producerRecord;
        // Determine the partition of the kafka send message based on the field name
        if (pluginConfig.hasPath(PARTITION_KEY)) {
            SeaTunnelRow keyElement = partitionExtractor.apply(element);
            producerRecord = seaTunnelRowSerializer.serializeRowByKey(keyElement, element);
        } else {
            producerRecord = seaTunnelRowSerializer.serializeRow(element);
        }
        kafkaProducerSender.send(producerRecord);
    }

    public KafkaSinkWriter(
            SinkWriter.Context context,
            SeaTunnelRowType seaTunnelRowType,
            Config pluginConfig,
            List<KafkaSinkState> kafkaStates) {
        this.context = context;
        this.pluginConfig = pluginConfig;
        this.topic = pluginConfig.getString(TOPIC);
        this.partitionKeys = createPartitionKeys(pluginConfig, seaTunnelRowType);
        this.partitionExtractor = createPartitionExtractor(seaTunnelRowType);
        if (pluginConfig.hasPath(PARTITION)) {
            this.partition = pluginConfig.getInt(PARTITION);
        }
        if (pluginConfig.hasPath(ASSIGN_PARTITIONS)) {
            MessageContentPartitioner.setAssignPartitions(pluginConfig.getStringList(ASSIGN_PARTITIONS));
        }
        if (pluginConfig.hasPath(TRANSACTION_PREFIX)) {
            this.transactionPrefix = pluginConfig.getString(TRANSACTION_PREFIX);
        } else {
            Random random = new Random();
            this.transactionPrefix = String.format("SeaTunnel%04d", random.nextInt(PREFIX_RANGE));
        }
        restoreState(kafkaStates);
        this.seaTunnelRowSerializer = getSerializer(pluginConfig, seaTunnelRowType);
        if (KafkaSemantics.EXACTLY_ONCE.equals(getKafkaSemantics(pluginConfig))) {
            this.kafkaProducerSender =
                    new KafkaTransactionSender<>(this.transactionPrefix, getKafkaProperties(pluginConfig));
            // abort all transaction number bigger than current transaction, because they maybe already start
            //  transaction.
            if (!kafkaStates.isEmpty()) {
                this.kafkaProducerSender.abortTransaction(kafkaStates.get(0).getCheckpointId() + 1);
            }
            this.kafkaProducerSender.beginTransaction(generateTransactionId(this.transactionPrefix,
                    this.lastCheckpointId + 1));
        } else {
            this.kafkaProducerSender = new KafkaNoTransactionSender<>(getKafkaProperties(pluginConfig));
        }
    }

    @Override
    public List<KafkaSinkState> snapshotState(long checkpointId) {
        List<KafkaSinkState> states = kafkaProducerSender.snapshotState(checkpointId);
        this.lastCheckpointId = checkpointId;
        this.kafkaProducerSender.beginTransaction(generateTransactionId(this.transactionPrefix,
                this.lastCheckpointId + 1));
        return states;
    }

    @Override
    public Optional<KafkaCommitInfo> prepareCommit() {
        return kafkaProducerSender.prepareCommit();
    }

    @Override
    public void abortPrepare() {
        kafkaProducerSender.abortTransaction();
    }

    @Override
    public void close() {
        try (KafkaProduceSender<?, ?> kafkaProduceSender = kafkaProducerSender) {
            // no-opt
        } catch (Exception e) {
            throw new RuntimeException("Close kafka sink writer error", e);
        }
    }

    private Properties getKafkaProperties(Config pluginConfig) {
        Config kafkaConfig = TypesafeConfigUtils.extractSubConfig(pluginConfig, PRODUCER_CONFIG_PREFIX, false);
        Properties kafkaProperties = new Properties();
        kafkaConfig.entrySet().forEach(entry -> kafkaProperties.put(entry.getKey(), entry.getValue().unwrapped()));
        if (pluginConfig.hasPath(ASSIGN_PARTITIONS)) {
            kafkaProperties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.apache.seatunnel.connectors.seatunnel.kafka.sink.MessageContentPartitioner");
        }
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, pluginConfig.getString(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return kafkaProperties;
    }

    // todo: parse the target field from config
    private SeaTunnelRowSerializer<byte[], byte[]> getSerializer(Config pluginConfig, SeaTunnelRowType seaTunnelRowType) {
        if (pluginConfig.hasPath(PARTITION)) {
            return new DefaultSeaTunnelRowSerializer(this.topic, this.partition, seaTunnelRowType);
        } else if (CollectionUtils.isNotEmpty(this.partitionKeys)) {
            int size = this.partitionKeys.size();
            String[] keyFieldNames = new String[size];
            SeaTunnelDataType<?>[] keyFieldTypes = new SeaTunnelDataType[size];
            int index = 0;
            for (int i = 0; i < seaTunnelRowType.getFieldNames().length; i++) {
                if (this.partitionKeys.contains(seaTunnelRowType.getFieldNames()[i])) {
                    keyFieldNames[index] = seaTunnelRowType.getFieldName(i);
                    keyFieldTypes[index] = seaTunnelRowType.getFieldType(i);
                    ++index;
                }
            }
            SeaTunnelRowType keySeaTunnelRowType = new SeaTunnelRowType(keyFieldNames, keyFieldTypes);
            return new DefaultSeaTunnelRowSerializer(this.topic, keySeaTunnelRowType, seaTunnelRowType);
        } else {
            return new DefaultSeaTunnelRowSerializer(this.topic, seaTunnelRowType);
        }
    }

    private KafkaSemantics getKafkaSemantics(Config pluginConfig) {
        if (pluginConfig.hasPath("semantics")) {
            return pluginConfig.getEnum(KafkaSemantics.class, "semantics");
        }
        return KafkaSemantics.NON;
    }

    protected static String generateTransactionId(String transactionPrefix, long checkpointId) {
        return transactionPrefix + "-" + checkpointId;
    }

    private void restoreState(List<KafkaSinkState> states) {
        if (!states.isEmpty()) {
            this.transactionPrefix = states.get(0).getTransactionIdPrefix();
            this.lastCheckpointId = states.get(0).getCheckpointId();
        }
    }

    private Function<SeaTunnelRow, SeaTunnelRow> createPartitionExtractor(SeaTunnelRowType seaTunnelRowType) {
        if (CollectionUtils.isEmpty(this.partitionKeys)) {
            return row -> null;
        }
        return row -> {
            SeaTunnelRow keySeaTunnelRow = new SeaTunnelRow(this.partitionKeys.size());
            int index = 0;
            for (int i = 0; i < seaTunnelRowType.getFieldNames().length; i++) {
                String fieldName = seaTunnelRowType.getFieldNames()[i];
                if (this.partitionKeys.contains(fieldName)) {
                    int partitionFieldIndex = seaTunnelRowType.indexOf(fieldName);
                    Object partitionFieldValue = row.getField(partitionFieldIndex);
                    keySeaTunnelRow.setField(index, partitionFieldValue);
                    ++index;
                }
            }
            return keySeaTunnelRow;
        };
    }

    private List<String> createPartitionKeys(Config pluginConfig, SeaTunnelRowType seaTunnelRowType) {
        if (pluginConfig.hasPath(PARTITION_KEY)) {
            return pluginConfig.getStringList(PARTITION_KEY).stream()
                    .filter(f -> {
                        if (Arrays.asList(seaTunnelRowType.getFieldNames()).contains(f)) {
                            return true;
                        } else {
                            log.warn("Not found partition key field: {}", f);
                            return false;
                        }
                    })
                    .collect(Collectors.toList());
        }
        return null;
    }
}
