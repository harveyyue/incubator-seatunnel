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

package org.apache.seatunnel.common.aviator;

import static org.apache.seatunnel.common.aviator.AviatorDateUtils.DATE_FORMAT_MAPPINGS;
import static org.apache.seatunnel.common.aviator.AviatorDateUtils.DATE_FUNCTION_PATTERNS;
import static org.apache.seatunnel.common.aviator.AviatorDateUtils.DIGITAL_PATTERN;

import org.apache.seatunnel.common.aviator.function.DateFunction;
import org.apache.seatunnel.common.aviator.function.HourFunction;
import org.apache.seatunnel.common.aviator.function.MinuteFunction;
import org.apache.seatunnel.common.aviator.function.MonthFunction;
import org.apache.seatunnel.common.aviator.function.QuarterFunction;
import org.apache.seatunnel.common.aviator.function.WeekFunction;
import org.apache.seatunnel.common.aviator.function.YearFunction;
import org.apache.seatunnel.common.utils.SeaTunnelException;

import com.googlecode.aviator.AviatorEvaluator;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class AviatorHelper {
    static {
        AviatorEvaluator.addFunction(new YearFunction());
        AviatorEvaluator.addFunction(new QuarterFunction());
        AviatorEvaluator.addFunction(new MonthFunction());
        AviatorEvaluator.addFunction(new WeekFunction());
        AviatorEvaluator.addFunction(new DateFunction());
        AviatorEvaluator.addFunction(new HourFunction());
        AviatorEvaluator.addFunction(new MinuteFunction());
    }

    public static Object execute(String functionExpression) {
        return AviatorEvaluator.execute(functionExpression);
    }

    public static Map<String, String> executeAviatorFunction(String expression) {
        Map<String, String> functions = new HashMap<>();
        for (Map.Entry<Pattern, String> entry : DATE_FUNCTION_PATTERNS.entrySet()) {
            Matcher matcher = entry.getKey().matcher(expression);
            if (matcher.matches()) {
                String result;
                String origin = matcher.group(1);
                int groupCount = matcher.groupCount();
                if (groupCount == 0) {
                    result = entry.getValue();
                } else {
                    String[] paras = new String[groupCount - 1];
                    for (int i = 2; i <= groupCount; i++) {
                        String group = matcher.group(i);
                        if (DIGITAL_PATTERN.matcher(group).matches()) {
                            paras[i - 2] = group.charAt(0) == '+' ? group.substring(1) : group;
                        } else {
                            paras[i - 2] = String.format("'%s'",
                                DATE_FORMAT_MAPPINGS.get(group) != null ? DATE_FORMAT_MAPPINGS.get(group) : group);
                        }
                    }
                    result = String.format(entry.getValue(), paras);
                }

                try {
                    log.info("execute function: {}", result);
                    functions.put(origin, (String) execute(result));
                } catch (Exception e) {
                    log.error("execute expression failed: ", e);
                    new SeaTunnelException("execute expression failed: " + e.getMessage());
                }
            }
        }
        return functions;
    }

    public static String parseExpression(String expression) {
        String result = expression;
        Map<String, String> functions = executeAviatorFunction(expression);
        for (Map.Entry<String, String> entry : functions.entrySet()) {
            result = result.replace(entry.getKey(), entry.getValue());
        }
        if (!result.equals(expression)) {
            log.info("parse expression from {} to {}", expression, result);
        }
        return result;
    }
}
