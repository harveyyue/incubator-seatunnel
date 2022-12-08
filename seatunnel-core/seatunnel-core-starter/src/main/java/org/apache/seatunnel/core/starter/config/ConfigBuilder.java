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

import org.apache.seatunnel.common.config.ConfigRuntimeException;
import org.apache.seatunnel.common.utils.SechubUtils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigParseOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigSyntax;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Used to build the {@link  Config} from file.
 */
@Slf4j
public class ConfigBuilder {

    public static final Pattern S3_PATH_PATTERN = Pattern.compile("s3:\\/\\/(.*?)\\/(.*)");
    private static final String PLUGIN_NAME_KEY = "plugin_name";
    private Path configFile;
    private final Config config;

    public ConfigBuilder(Path configFile) {
        this.configFile = configFile;
        this.config = load();
    }

    public ConfigBuilder(String s3AccessKeyId, String s3SecretAccessKey, String s3Region, String configPath) {
        this.config = loadFromS3(s3AccessKeyId, s3SecretAccessKey, s3Region, configPath);
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

        return config;
    }

    private Config loadFromS3(String s3AccessKeyId, String s3SecretAccessKey, String s3Region, String configPath) {
        // parse s3 bucket and file path
        Matcher matcher = S3_PATH_PATTERN.matcher(configPath);
        String bucketName;
        String key;
        if (matcher.matches()) {
            bucketName = matcher.group(1);
            key = matcher.group(2);
        } else {
            throw new ConfigRuntimeException("S3 path invalid: " + configPath);
        }

        // decrypt s3 secret access key
        matcher = SechubUtils.SECHUB_PATTERN.matcher(s3SecretAccessKey);
        if (matcher.matches()) {
            String secKey = matcher.group(1);
            String encryptText = matcher.group(2);
            try {
                s3SecretAccessKey = SechubUtils.decrypt(SechubUtils.CRYPTO_TYPE, secKey, encryptText);
            } catch (Exception e) {
                throw new ConfigRuntimeException("Sechub decrypt error: ", e);
            }
        }

        // parse config file extension
        String extension = getFileExtension(configPath);
        ConfigSyntax configSyntax;
        switch (extension) {
            case "CONF":
                configSyntax = ConfigSyntax.CONF;
                break;
            case "JSON":
                configSyntax = ConfigSyntax.JSON;
                break;
            case "PROPERTIES":
                configSyntax = ConfigSyntax.PROPERTIES;
                break;
            default:
                configSyntax = ConfigSyntax.CONF;
                log.info("Config file extension {} is not supported, using CONF format instead", extension);
                break;
        }

        // read config values from s3
        StaticCredentialsProvider staticCredentialsProvider = StaticCredentialsProvider.create(
            AwsBasicCredentials.create(s3AccessKeyId, s3SecretAccessKey));
        S3Client s3Client = S3Client.builder()
            .credentialsProvider(staticCredentialsProvider)
            .region(s3Region == null ? Region.AP_SOUTHEAST_1 : Region.of(s3Region))
            .build();
        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(key).build();
        try (ResponseInputStream<GetObjectResponse> responseInputStream = s3Client.getObject(getObjectRequest);
             BufferedReader reader = new BufferedReader(new InputStreamReader(responseInputStream, "UTF-8"))) {
            ConfigParseOptions configParseOptions = ConfigParseOptions.defaults().setSyntax(configSyntax).setAllowMissing(true);
            Config config = ConfigFactory
                .parseReader(reader, configParseOptions)
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                .resolveWith(ConfigFactory.systemProperties(),
                    ConfigResolveOptions.defaults().setAllowUnresolved(true));

            ConfigRenderOptions options = ConfigRenderOptions.concise().setFormatted(true);
            log.info("parsed s3 config file: {}", config.root().render(options));

            return config;
        } catch (IOException ex) {
            throw new ConfigRuntimeException("Read s3 file failed: " + ex.getMessage());
        }
    }

    public Config getConfig() {
        return config;
    }

    private String getFileExtension(String path) {
        int pos = path.lastIndexOf(".");
        return path.substring(pos + 1).toUpperCase();
    }

}
