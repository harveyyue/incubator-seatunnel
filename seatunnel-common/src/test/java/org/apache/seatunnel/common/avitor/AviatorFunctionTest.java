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

import static org.apache.seatunnel.common.aviator.AviatorDateUtils.addDate;
import static org.apache.seatunnel.common.aviator.AviatorDateUtils.addHour;
import static org.apache.seatunnel.common.aviator.AviatorDateUtils.addMinute;
import static org.apache.seatunnel.common.aviator.AviatorDateUtils.addMonth;
import static org.apache.seatunnel.common.aviator.AviatorDateUtils.addQuarter;
import static org.apache.seatunnel.common.aviator.AviatorDateUtils.addYear;
import static org.apache.seatunnel.common.aviator.AviatorDateUtils.quarterDate;
import static org.apache.seatunnel.common.aviator.AviatorDateUtils.quarterOfYear;
import static org.apache.seatunnel.common.aviator.AviatorDateUtils.weekDate;
import static org.apache.seatunnel.common.aviator.AviatorDateUtils.weekOfYear;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.seatunnel.common.aviator.AviatorHelper;
import org.apache.seatunnel.common.aviator.DateDirectionEnum;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.Date;

public class AviatorFunctionTest {

    @Test
    public void testYearFunction() {
        assertEquals(AviatorHelper.execute("year()"), String.valueOf(LocalDate.now().getYear()));
        assertEquals(AviatorHelper.execute("year(-1)"), addYear(-1));
        assertEquals(AviatorHelper.execute("year(1)"), addYear(1));
    }

    @Test
    public void testYearExpression() {
        assertEquals(AviatorHelper.parseExpression("${year}"), String.valueOf(LocalDate.now().getYear()));
        assertEquals(AviatorHelper.parseExpression("${year+1}"), addYear(1));
        assertEquals(AviatorHelper.parseExpression("${year-1}"), addYear(-1));
    }

    @Test
    public void testMonthFunction() {
        String month = (String) AviatorHelper.execute("month()");
        assertEquals(month, DateFormatUtils.format(new Date(), "yyyyMM"));

        month = (String) AviatorHelper.execute("month(1)");
        assertEquals(month, addMonth(1, "yyyyMM"));

        month = (String) AviatorHelper.execute("month(-1, 'yyyy-MM')");
        assertEquals(month, addMonth(-1, "yyyy-MM"));

        String monthOfStart = (String) AviatorHelper.execute("month(0, 'start', 0, 'yyyy-MM-dd')");
        String monthOfEnd = (String) AviatorHelper.execute("month(0, 'end', 0, 'yyyy-MM-dd')");
        String monthOfEnd2 = (String) AviatorHelper.execute("month(-1, 'end', -2, 'yyyy-MM-dd')");
        assertNotNull(monthOfStart);
        assertNotNull(monthOfEnd);
        assertNotNull(monthOfEnd2);
    }

    @Test
    public void testMonthExpression() {
        assertEquals(AviatorHelper.parseExpression("${month}"), DateFormatUtils.format(new Date(), "yyyyMM"));
        assertEquals(AviatorHelper.parseExpression("${month-1}"), addMonth(-1, "yyyyMM"));
        assertEquals(AviatorHelper.parseExpression("${month+1:y-m}"), addMonth(1, "yyyy-MM"));
        assertEquals(AviatorHelper.parseExpression("${month+2:ym}"), addMonth(2, "yyyyMM"));
        assertNotNull(AviatorHelper.parseExpression("${month.start}"));
        assertNotNull(AviatorHelper.parseExpression("${month.end}"));
        assertNotNull(AviatorHelper.parseExpression("${month-1.end-2:y-m-d}"));
    }

    @Test
    public void testWeekFunction() {
        Calendar calendar = Calendar.getInstance();
        String week = (String) AviatorHelper.execute("week()");
        assertEquals(week, weekOfYear());

        String weekOfFirst = (String) AviatorHelper.execute("week(\"start\")");
        calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
        assertEquals(weekOfFirst, DateFormatUtils.format(calendar.getTime(), "yyyyMMdd"));

        String weekOfEnd = (String) AviatorHelper.execute("week('end')");
        calendar.add(Calendar.WEEK_OF_MONTH, 1);
        calendar.add(Calendar.DAY_OF_WEEK, -1);
        assertEquals(weekOfEnd, DateFormatUtils.format(calendar.getTime(), "yyyyMMdd"));
    }

    @Test
    public void testWeekExpression() {
        assertEquals(AviatorHelper.parseExpression("${week}"), weekOfYear());
        assertEquals(AviatorHelper.parseExpression("${week.start}"), weekDate(DateDirectionEnum.START, "yyyyMMdd"));
        assertEquals(AviatorHelper.parseExpression("${week.end}"), weekDate(DateDirectionEnum.END, "yyyyMMdd"));
        assertEquals(AviatorHelper.parseExpression("${week.start:y-m-d}"), weekDate(DateDirectionEnum.START, "yyyy-MM-dd"));
    }

    @Test
    public void testDateFunction() {
        String date = (String) AviatorHelper.execute("date()");
        assertEquals(date, DateFormatUtils.format(new Date(), "yyyy-MM-dd"));

        date = (String) AviatorHelper.execute("date(1)");
        assertEquals(date, addDate(1, "yyyy-MM-dd"));

        date = (String) AviatorHelper.execute("date(-1)");
        assertEquals(date, addDate(-1, "yyyy-MM-dd"));
    }

    @Test
    public void testDateExpression() {
        assertEquals(AviatorHelper.parseExpression("${date}"), DateFormatUtils.format(new Date(), "yyyy-MM-dd"));
        assertEquals(AviatorHelper.parseExpression("${date+1}"), addDate(1, "yyyy-MM-dd"));
        assertEquals(AviatorHelper.parseExpression("${date-1}"), addDate(-1, "yyyy-MM-dd"));
    }

    @Test
    public void testHourFunction() {
        String hour = (String) AviatorHelper.execute("hour()");
        assertEquals(hour, String.valueOf(LocalDateTime.now().getHour()));

        hour = (String) AviatorHelper.execute("hour(1)");
        assertEquals(hour, String.valueOf(LocalDateTime.now().plusHours(1).getHour()));

        hour = (String) AviatorHelper.execute("hour(-1)");
        assertEquals(hour, String.valueOf(LocalDateTime.now().plusHours(-1).getHour()));

        hour = (String) AviatorHelper.execute("hour(1, 'yyyyMMddHH')");
        assertNotNull(hour);
    }

    @Test
    public void testHourExpression() {
        assertEquals(AviatorHelper.parseExpression("${hour}"), addHour(0));
        assertEquals(AviatorHelper.parseExpression("${hour-1}"), addHour(-1));
        assertEquals(AviatorHelper.parseExpression("${hour-1:ymdh}"), addHour(-1, "yyyyMMddHH"));
    }

    @Test
    public void testMinuteFunction() {
        String minute = (String) AviatorHelper.execute("minute()");
        assertEquals(minute, addMinute(0));

        minute = (String) AviatorHelper.execute("minute(1)");
        assertEquals(minute, addMinute(1));

        minute = (String) AviatorHelper.execute("minute(-1)");
        assertEquals(minute, addMinute(-1));

        minute = (String) AviatorHelper.execute("minute(-1, 'yyyyMMddHHmm')");
        assertEquals(minute, addMinute(-1, "yyyyMMddHHmm"));
    }

    @Test
    public void testMinuteExpression() {
        assertEquals(AviatorHelper.parseExpression("${minute}"), addMinute(0));
        assertEquals(AviatorHelper.parseExpression("${minute-1}"), addMinute(-1));
        assertEquals(AviatorHelper.parseExpression("${minute-1:ymdhi}"), addMinute(-1, "yyyyMMddHHmm"));
    }

    @Test
    public void testQuarterFunction() {
        String quarter = (String) AviatorHelper.execute("quarter()");
        assertEquals(quarter, quarterOfYear());

        quarter = (String) AviatorHelper.execute("quarter(1)");
        assertEquals(quarter, addQuarter(1));

        quarter = (String) AviatorHelper.execute("quarter(-1)");
        assertEquals(quarter, addQuarter(-1));

        quarter = (String) AviatorHelper.execute("quarter('start', 0, 'yyyyMMdd')");
        assertNotNull(quarter);

        quarter = (String) AviatorHelper.execute("quarter('start', 1, 'yyyy-MM-dd')");
        assertNotNull(quarter);
    }

    @Test
    public void testQuarterExpression() {
        assertEquals(AviatorHelper.parseExpression("${quarter}"), quarterOfYear());
        assertEquals(AviatorHelper.parseExpression("${quarter-1}"), addQuarter(-1));
        assertEquals(AviatorHelper.parseExpression("${quarter.start}"), quarterDate(DateDirectionEnum.START, 0, "yyyyMMdd"));
        assertEquals(AviatorHelper.parseExpression("${quarter.end}"), quarterDate(DateDirectionEnum.END, 0, "yyyyMMdd"));
        assertEquals(AviatorHelper.parseExpression("${quarter.start+1:y-m-d}"), quarterDate(DateDirectionEnum.START, 1, "yyyy-MM-dd"));
        assertEquals(AviatorHelper.parseExpression("${quarter.end-2:y-m-d}"), quarterDate(DateDirectionEnum.END, -2, "yyyy-MM-dd"));
    }
}
