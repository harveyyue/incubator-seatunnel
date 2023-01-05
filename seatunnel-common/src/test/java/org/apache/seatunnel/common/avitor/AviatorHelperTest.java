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

public class AviatorHelperTest {

    @Test
    public void testMatchDateFunction() {
        String query = "select * from workdb.test where dt = '${date-1}' and hh = '${hour-1:ymdh}'";
        String replacedQuery = AviatorHelper.parseExpression(query);
        Assertions.assertNotEquals(query, replacedQuery);

        query = "select * from workdb.test where year_1 = '${year}' and year_2 = '${year}'";
        replacedQuery = AviatorHelper.parseExpression(query);
        Assertions.assertNotEquals(query, replacedQuery);
        Assertions.assertFalse(AviatorHelper.isAviatorFunction(replacedQuery));
    }

    @Test
    public void testMatchDateFunctionWithFormat() {
        String query = "select * from workdb.test where dt = '${month+1:y-m}' and dt_2='${month-1.end-2:y-m-d}'";
        String replacedQuery = AviatorHelper.parseExpression(query);
        Assertions.assertNotEquals(query, replacedQuery);
        Assertions.assertFalse(AviatorHelper.isAviatorFunction(replacedQuery));

        query = "select * from workdb.test where dt = '${month+1}' and dt_2='${month-1.end-2:y-m-d}'";
        replacedQuery = AviatorHelper.parseExpression(query);
        Assertions.assertNotEquals(query, replacedQuery);
        Assertions.assertFalse(AviatorHelper.isAviatorFunction(replacedQuery));

        query = "select * from workdb.test where dt = '${month+1}' and dt_2='${quarter.start+1:y-m-d}' and dt_3='${minute-1:ymdhi}'";
        replacedQuery = AviatorHelper.parseExpression(query);
        Assertions.assertNotEquals(query, replacedQuery);
        Assertions.assertFalse(AviatorHelper.isAviatorFunction(replacedQuery));
    }

    @Test
    public void testAllFunctionsFromExpression() {
        String query = "select\n" +
                "id,\n" +
                "foo,\n" +
                "bar\n" +
                "from\n" +
                "source_table\n" +
                "where\n" +
                "foo = '${year}'\n" +
                "or foo = '${year-1}'\n" +
                "or foo = '${year+1}'\n" +
                "or foo = '${month}'\n" +
                "or foo = '${month-1}'\n" +
                "or foo = '${month+1:y-m}'\n" +
                "or foo = '${month.start}'\n" +
                "or foo = '${month.end}'\n" +
                "or foo = '${month-1.end-2:y-m-d}'\n" +
                "or foo = '${week}'\n" +
                "or foo = '${week.start}'\n" +
                "or foo = '${week.end}'\n" +
                "or foo = '${date}'\n" +
                "or foo = '${date-1}'\n" +
                "or foo = '${date+1}'\n" +
                "or foo = '${hour}'\n" +
                "or foo = '${hour-1}'\n" +
                "or foo = '${hour-1:ymdh}'\n" +
                "or foo = '${minute}'\n" +
                "or foo = '${minute-1}'\n" +
                "or foo = '${minute-1:ymdhi}'\n" +
                "or foo = '${quarter}'\n" +
                "or foo = '${quarter-1}'\n" +
                "or foo = '${quarter.start}'\n" +
                "or foo = '${quarter.end}'\n" +
                "or foo = '${quarter.start+1:y-m-d}'\n";
        String replacedQuery = AviatorHelper.parseExpression(query);
        Assertions.assertFalse(AviatorHelper.isAviatorFunction(replacedQuery));
    }

    @Test
    public void testNotMatchDateFunction() {
        String query = "select * from workdb.test where dt = '2022-12-19' and hh = '16'";
        String replacedQuery = AviatorHelper.parseExpression(query);
        Assertions.assertEquals(query, replacedQuery);
    }
}