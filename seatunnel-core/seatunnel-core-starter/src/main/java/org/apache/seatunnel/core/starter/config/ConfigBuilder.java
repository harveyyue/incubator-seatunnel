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

package org.apache.seatunnel.core.starter.config;

import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.ConfigRuntimeException;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.common.utils.SechubUtils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueType;

import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Used to build the {@link  Config} from file.
 */
@Slf4j
public class ConfigBuilder {

    public static final Pattern SECHUB_PATTERN = Pattern.compile("\\$\\{sechub:(.*):(.*)\\}");
    private static final String PLUGIN_NAME_KEY = "plugin_name";
    private final Path configFile;
    private final Config config;

    public ConfigBuilder(Path configFile) {
        this.configFile = configFile;
        this.config = load();
    }

    private Config load() {

        if (configFile == null) {
            throw new ConfigRuntimeException("Please specify config file");
        }

        log.info("Loading config file: {}", configFile);

        // variables substitution / variables resolution order:
        // config file --> system environment --> java properties
        Config config = ConfigFactory
            .parseFile(configFile.toFile())
            .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
            .resolveWith(ConfigFactory.systemProperties(),
                ConfigResolveOptions.defaults().setAllowUnresolved(true));

        ConfigRenderOptions options = ConfigRenderOptions.concise().setFormatted(true);
        log.info("parsed config file: {}", config.root().render(options));

        List<? extends Config> sourceConfigs = config.getConfigList(Constants.SOURCE);
        List<? extends Config> sinkConfigs = config.getConfigList(Constants.SINK);
        Map<String, String> sechubMap = sechubDecryptValues(sourceConfigs);
        sechubMap.putAll(sechubDecryptValues(sinkConfigs));
        if (sechubMap.size() > 0) {
            log.info("Sechub decrypt the relevant config");
            String rawConfig = FileUtils.readFileToStr(configFile);
            for (Map.Entry<String, String> entry : sechubMap.entrySet()) {
                rawConfig = rawConfig.replace(entry.getKey(), entry.getValue());
            }
            return load(rawConfig);
        }
        return config;
    }

    private Config load(String data) {
        return ConfigFactory
            .parseString(data)
            .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
            .resolveWith(ConfigFactory.systemProperties(),
                ConfigResolveOptions.defaults().setAllowUnresolved(true));
    }

    private Map<String, String> sechubDecryptValues(List<? extends Config> configs) {
        Map<String, String> result = new HashMap<>();
        configs.forEach(config ->
            config.entrySet().forEach(entry -> {
                ConfigValue value = entry.getValue();
                Matcher matcher = SECHUB_PATTERN.matcher(value.unwrapped().toString());
                if (value.valueType().equals(ConfigValueType.STRING) && matcher.matches()) {
                    String secKey = matcher.group(1);
                    String decryptText = matcher.group(2);
                    try {
                        String plainText = SechubUtils.decrypt(SechubUtils.CRYPTO_TYPE, secKey, decryptText);
                        result.put(value.unwrapped().toString(), plainText);
                    } catch (Exception e) {
                        throw new ConfigRuntimeException("Sechub decrypt error: ", e);
                    }
                }
            })
        );
        return result;
    }

    public Config getConfig() {
        return config;
    }

}
