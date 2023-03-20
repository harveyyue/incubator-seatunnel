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

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hudi.util.HudiUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class HudiSourceSplitEnumerator implements SourceSplitEnumerator<HudiSourceSplit, HudiSourceState> {

    private static final String HUDI_METADATA_FOLDER = ".hoodie";
    private final Context<HudiSourceSplit> context;
    private Set<HudiSourceSplit> pendingSplit;
    private Set<HudiSourceSplit> assignedSplit;
    private final String tablePath;
    private final HadoopConf hadoopConf;
    private final List<String> readPartitions;

    public HudiSourceSplitEnumerator(SourceSplitEnumerator.Context<HudiSourceSplit> context,
                                     String tablePath,
                                     HadoopConf hadoopConf,
                                     List<String> readPartitions) {
        this.context = context;
        this.tablePath = tablePath;
        this.hadoopConf = hadoopConf;
        this.readPartitions = readPartitions;
    }

    public HudiSourceSplitEnumerator(SourceSplitEnumerator.Context<HudiSourceSplit> context,
                                     String tablePath,
                                     HadoopConf hadoopConf,
                                     List<String> readPartitions,
                                     HudiSourceState sourceState) {
        this(context, tablePath, hadoopConf, readPartitions);
        this.assignedSplit = sourceState.getAssignedSplit();
    }

    @Override
    public void open() {
        this.assignedSplit = new HashSet<>();
        this.pendingSplit = new HashSet<>();
    }

    @Override
    public void run() throws Exception {
        pendingSplit = getHudiSplit();
        assignSplit(context.registeredReaders());
    }

    private Set<HudiSourceSplit> getHudiSplit() throws IOException {
        Set<HudiSourceSplit> hudiSourceSplits = new HashSet<>();
        String commaSeparatedPaths = tablePath;
        FileSystem fs =  FSUtils.getFs(tablePath, HudiUtil.getConfiguration(hadoopConf));
        Option<String[]> partitionFields = getPartitionFields(fs, tablePath);
        if (partitionFields.isPresent() && partitionFields.get().length > 0) {
            Pattern partitionPattern = getPartitionPattern(partitionFields.get());
            List<String> partitionPaths = getPartitionPaths(fs, tablePath, partitionPattern);
            if (partitionPaths.isEmpty()) {
                throw new HudiConnectorException(HudiConnectorErrorCode.NO_SUITABLE_PARTITION_PATH,
                    "Please specify the appropriate partition path to property read_partitions");
            }
            commaSeparatedPaths = partitionPaths.stream().collect(Collectors.joining(","));
        }
        Configuration configuration = HudiUtil.getConfiguration(hadoopConf);
        JobConf jobConf = HudiUtil.toJobConf(configuration);
        FileInputFormat.setInputPaths(jobConf, commaSeparatedPaths);
        HoodieParquetInputFormat inputFormat = new HoodieParquetInputFormat();
        inputFormat.setConf(jobConf);
        InputSplit[] inputSplits = inputFormat.getSplits(jobConf, 0);
        log.info("Reading hudi input splits: {}",
            Arrays.stream(inputSplits).map(InputSplit::toString).collect(Collectors.joining(",")));
        for (InputSplit split : inputSplits) {
            hudiSourceSplits.add(new HudiSourceSplit(split.toString(), split));
        }
        return hudiSourceSplits;
    }

    private Option<String[]> getPartitionFields(FileSystem fs, String basePath) {
        basePath = basePath.endsWith("/") ? basePath : basePath + "/";
        HoodieTableConfig hoodieTableConfig = new HoodieTableConfig(fs, basePath + HUDI_METADATA_FOLDER, null);
        return hoodieTableConfig.getPartitionFields();
    }

    private Pattern getPartitionPattern(String[] partitions) {
        String partitionExpression = Arrays.stream(partitions).map(pt -> String.format("%s=(.*?)", pt)).collect(Collectors.joining("\\/"));
        return Pattern.compile(".*\\/" + partitionExpression);
    }

    private List<String> getPartitionPaths(FileSystem fs, String path, Pattern partitionPattern) throws IOException {
        List<String> paths = new ArrayList<>();
        FileStatus[] fileStatuses = fs.listStatus(new Path(path));
        for (FileStatus fileStatus: fileStatuses) {
            if (fileStatus.isDirectory()) {
                String currentPath = fileStatus.getPath().toString();
                if (partitionPattern.matcher(currentPath).matches()) {
                    if (!readPartitions.isEmpty()) {
                        for (String readPartition : readPartitions) {
                            if (currentPath.contains(readPartition)) {
                                paths.add(currentPath);
                                break;
                            }
                        }
                    } else {
                        paths.add(currentPath);
                    }
                } else {
                    paths.addAll(getPartitionPaths(fs, fileStatus.getPath().toString(), partitionPattern));
                }
            }
        }
        return paths;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void addSplitsBack(List<HudiSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            pendingSplit.addAll(splits);
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    private void assignSplit(Collection<Integer> taskIdList) {
        Map<Integer, List<HudiSourceSplit>> readySplit = new HashMap<>(Common.COLLECTION_SIZE);
        for (int taskId : taskIdList) {
            readySplit.computeIfAbsent(taskId, id -> new ArrayList<>());
        }

        pendingSplit.forEach(s -> readySplit.get(getSplitOwner(s.splitId(), taskIdList.size())).add(s));
        readySplit.forEach(context::assignSplit);
        assignedSplit.addAll(pendingSplit);
        pendingSplit.clear();
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplit.size();
    }

    @Override
    public void registerReader(int subtaskId) {
        if (!pendingSplit.isEmpty()) {
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public HudiSourceState snapshotState(long checkpointId) {
        return new HudiSourceState(assignedSplit);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {

    }

    @Override
    public void handleSplitRequest(int subtaskId) {

    }
}
