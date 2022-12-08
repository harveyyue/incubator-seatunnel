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

package org.apache.seatunnel.common.utils;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.regex.Pattern;

@SuppressWarnings("checkstyle:MagicNumber")
public class SechubUtils {
    public static final Pattern SECHUB_PATTERN = Pattern.compile("\\$\\{sechub:(.*):(.*)\\}");
    public static final String CRYPTO_TYPE = "ase256gcm";
    private static final byte[] IV = new byte[12];
    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();
    private static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    public static String encrypt(String cryptoType, String key, String content) throws Exception {
        if (cryptoType == null) {
            throw new RuntimeException("cryptoType is null");
        } else {
            switch (cryptoType) {
                case "ase256gcm":
                case "aes256gcm":
                    SecretKeySpec secretKey;
                    if (key.length() == 64) {
                        secretKey = new SecretKeySpec(hexStringToByteArray(key), "AES");
                    } else {
                        secretKey = new SecretKeySpec(key.getBytes(), "AES");
                    }

                    return encrypt(content, secretKey, IV);
                default:
                    throw new RuntimeException("cryptoType:" + cryptoType + " is not support");
            }
        }
    }

    public static String decrypt(String cryptoType, String key, String content) throws Exception {
        if (cryptoType == null) {
            throw new RuntimeException("cryptoType is null");
        } else {
            switch (cryptoType) {
                case "ase256gcm":
                case "aes256gcm":
                    SecretKeySpec secretKey;
                    if (key.length() == 64) {
                        secretKey = new SecretKeySpec(hexStringToByteArray(key), "AES");
                    } else {
                        secretKey = new SecretKeySpec(key.getBytes(), "AES");
                    }

                    return decrypt(content, secretKey, IV);
                default:
                    throw new RuntimeException("cryptoType:" + cryptoType + " is not support");
            }
        }
    }

    private static String encrypt(String plaintext, SecretKey key, byte[] iv) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidAlgorithmParameterException, InvalidKeyException, BadPaddingException, IllegalBlockSizeException {
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        SecretKeySpec keySpec = new SecretKeySpec(key.getEncoded(), "AES");
        byte[] gcmIV = new byte[12];
        SECURE_RANDOM.nextBytes(gcmIV);
        GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(128, gcmIV);
        cipher.init(1, keySpec, gcmParameterSpec);
        byte[] cipherText = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));
        ByteBuffer byteBuffer = ByteBuffer.allocate(iv.length + cipherText.length);
        byteBuffer.put(gcmIV);
        byteBuffer.put(cipherText);
        byte[] cipherMessage = byteBuffer.array();
        return BASE64_ENCODER.encodeToString(cipherMessage);
    }

    private static String decrypt(String cipherText, SecretKey key, byte[] iv) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        byte[] cipherMessage = BASE64_DECODER.decode(cipherText);
        SecretKeySpec keySpec = new SecretKeySpec(key.getEncoded(), "AES");

        byte[] decryptedText;
        try {
            GCMParameterSpec gcmIV = new GCMParameterSpec(128, cipherMessage, 0, 12);
            cipher.init(2, keySpec, gcmIV);
            decryptedText = cipher.doFinal(cipherMessage, 12, cipherMessage.length - 12);
        } catch (Exception var9) {
            GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(128, iv);
            cipher.init(2, keySpec, gcmParameterSpec);
            decryptedText = cipher.doFinal(cipherMessage);
        }

        return new String(decryptedText);
    }

    private static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];

        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
        }

        return data;
    }
}
