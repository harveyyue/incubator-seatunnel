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

package org.apache.seatunnel.core.starter.command;

import static org.apache.seatunnel.core.starter.utils.FileUtils.checkConfigExist;

import org.apache.seatunnel.core.starter.config.ConfigBuilder;
import org.apache.seatunnel.core.starter.exception.CommandExecuteException;
import org.apache.seatunnel.core.starter.exception.ConfigCheckException;
import org.apache.seatunnel.core.starter.utils.FileUtils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.nio.file.Path;

/**
 * Command interface.
 *
 * @param <T> args type
 */
@FunctionalInterface
public interface Command<T extends CommandArgs> {

    /**
     * Execute command
     *
     */
    void execute() throws CommandExecuteException, ConfigCheckException;

    default Config getConfig(AbstractCommandArgs commandArgs) {
        ConfigBuilder configBuilder;
        if (commandArgs.enableS3Config()) {
            configBuilder = new ConfigBuilder(commandArgs.getS3AccessKeyId(), commandArgs.getS3SecretAccessKey(),
                    commandArgs.getS3Region(), commandArgs.getConfigFile());
        } else {
            Path configFile = FileUtils.getConfigPath(commandArgs);
            checkConfigExist(configFile);
            configBuilder = new ConfigBuilder(configFile);
        }
        return configBuilder.getConfig();
    }
}
