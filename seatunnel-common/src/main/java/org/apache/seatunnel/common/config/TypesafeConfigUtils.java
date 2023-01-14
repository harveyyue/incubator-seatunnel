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

package org.apache.seatunnel.common.config;

import org.apache.seatunnel.common.utils.SechubUtils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueType;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

@Slf4j
public final class TypesafeConfigUtils {

    private TypesafeConfigUtils() {
    }

    /**
     * Extract sub config with fixed prefix
     *
     * @param source     config source
     * @param prefix     config prefix
     * @param keepPrefix true if keep prefix
     */
    public static Config extractSubConfig(Config source, String prefix, boolean keepPrefix) {

        // use LinkedHashMap to keep insertion order
        Map<String, String> values = new LinkedHashMap<>();

        for (Map.Entry<String, ConfigValue> entry : source.entrySet()) {
            final String key = entry.getKey();
            final String value = String.valueOf(entry.getValue().unwrapped());

            if (key.startsWith(prefix)) {

                if (keepPrefix) {
                    values.put(key, value);
                } else {
                    values.put(key.substring(prefix.length()), value);
                }
            }
        }

        return ConfigFactory.parseMap(values);
    }

    /**
     * Check if config with specific prefix exists
     *
     * @param source config source
     * @param prefix config prefix
     * @return true if it has sub config
     */
    public static boolean hasSubConfig(Config source, String prefix) {

        boolean hasConfig = false;

        for (Map.Entry<String, ConfigValue> entry : source.entrySet()) {
            final String key = entry.getKey();

            if (key.startsWith(prefix)) {
                hasConfig = true;
                break;
            }
        }

        return hasConfig;
    }

    public static Config extractSubConfigThrowable(Config source, String prefix, boolean keepPrefix) {

        Config config = extractSubConfig(source, prefix, keepPrefix);

        if (config.isEmpty()) {
            throw new ConfigRuntimeException("config is empty");
        }

        return config;
    }

    @SuppressWarnings("unchecked")
    public static <T> T getConfig(final Config config, final String configKey, @NonNull final T defaultValue) {
        if (defaultValue.getClass().equals(Long.class)) {
            return config.hasPath(configKey) ? (T) Long.valueOf(config.getString(configKey)) : defaultValue;
        }
        if (defaultValue.getClass().equals(Integer.class)) {
            return config.hasPath(configKey) ? (T) Integer.valueOf(config.getString(configKey)) : defaultValue;
        }
        if (defaultValue.getClass().equals(String.class)) {
            return config.hasPath(configKey) ? (T) config.getString(configKey) : defaultValue;
        }
        if (defaultValue.getClass().equals(Boolean.class)) {
            return config.hasPath(configKey) ? (T) Boolean.valueOf(config.getString(configKey)) : defaultValue;
        }
        throw new RuntimeException("Unsupported config type, configKey: " + configKey);
    }

    public static List<? extends Config> getConfigList(Config config, String configKey) {
        return getConfigList(config, configKey, Collections.emptyList());
    }

    public static List<? extends Config> getConfigList(Config config,
                                                       String configKey,
                                                       @NonNull List<? extends Config> defaultValue) {
        if (!config.hasPath(configKey)) {
            return defaultValue;
        }

        List<? extends Config> configList = config.getConfigList(configKey);
        Map<String, String> plainValues = decryptConfigs(configList);
        if (plainValues.size() > 0) {
            log.info("Decrypt path: {}, decrypt {} properties", configKey, plainValues.size());
            configList = configList.stream()
                .map(m -> {
                    Config result = m;
                    for (Map.Entry<String, String> entry : plainValues.entrySet()) {
                        result = result.withValue(entry.getKey(), ConfigValueFactory.fromAnyRef(entry.getValue()));
                    }
                    return result;
                })
                .collect(Collectors.toList());
        }
        return configList;
    }

    private static Map<String, String> decryptConfigs(List<? extends Config> configs) {
        Map<String, String> result = new HashMap<>();
        configs.forEach(config ->
            config.entrySet().forEach(entry -> {
                ConfigValue value = entry.getValue();
                if (value.valueType().equals(ConfigValueType.STRING)) {
                    Matcher matcher = SechubUtils.SECHUB_PATTERN.matcher(value.unwrapped().toString());
                    if (matcher.matches()) {
                        String secKey = matcher.group(1);
                        String encryptText = matcher.group(2);
                        try {
                            String plainText = SechubUtils.decrypt(SechubUtils.CRYPTO_TYPE, secKey, encryptText);
                            result.put(entry.getKey(), plainText);
                        } catch (Exception e) {
                            throw new ConfigRuntimeException("Sechub decrypt error: ", e);
                        }
                    }
                }
            })
        );
        return result;
    }

    public static Map<String, String> extractPluginConfig(Config pluginConfig) {
        Map<String, String> result = new HashMap<>();
        pluginConfig.entrySet().forEach(entry -> result.put(entry.getKey(), entry.getValue().unwrapped().toString()));
        return result;
    }
}
