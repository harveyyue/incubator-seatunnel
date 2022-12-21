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

package org.apache.seatunnel.common.avitor;

import org.apache.seatunnel.common.aviator.AviatorHelper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AviatorFunctionTest {
    private static final Pattern FUNCTION_PATTERN = Pattern.compile(".*(\\$\\{.*\\}).*");

    @Test
    public void testMatchDateFunction() {
        String query = "select * from workdb.test where dt = '${date-1}' and hh = '${hour-1:ymdh}'";
        String replacedQuery = AviatorHelper.parseExpression(query);
        Assertions.assertNotEquals(query, replacedQuery);

        query = "select * from workdb.test where year_1 = '${year}' and year_2 = '${year}'";
        replacedQuery = AviatorHelper.parseExpression(query);
        Assertions.assertNotEquals(query, replacedQuery);
        Matcher matcher = FUNCTION_PATTERN.matcher(replacedQuery);
        Assertions.assertFalse(matcher.matches());
    }

    @Test
    public void testNotMatchDateFunction() {
        String query = "select * from workdb.test where dt = '2022-12-19' and hh = '16'";
        String replacedQuery = AviatorHelper.parseExpression(query);
        Assertions.assertEquals(query, replacedQuery);
    }
}
