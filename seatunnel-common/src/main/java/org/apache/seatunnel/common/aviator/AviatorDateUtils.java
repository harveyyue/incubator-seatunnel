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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

@Slf4j
@SuppressWarnings("MagicNumber")
public class AviatorDateUtils {
    public static final String MONTH_PATTERN = "yyyyMM";
    public static final String MONTH_STRIKE_PATTERN = "yyyy-MM";
    public static final String DAY_PATTERN = "yyyyMMdd";
    public static final String DAY_STRIKE_PATTERN = "yyyy-MM-dd";
    public static final String HOUR_PATTERN = "yyyyMMddHH";
    public static final String MINUTE_PATTERN = "yyyyMMddHHmm";

    public static final Pattern DIGITAL_PATTERN = Pattern.compile("[+-]?[0-9]+");
    public static final Map<Pattern, String> DATE_FUNCTION_PATTERNS = new HashMap<>();
    public static final Map<String, String> DATE_FORMAT_MAPPINGS = new HashMap<>();

    static {
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{year\\}).*"), "year()");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{year([+-]?[0-9]+)\\}).*"), "year(%s)");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{month\\}).*"), "month()");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{month([+-]?[0-9]+)\\}).*"), "month(%s)");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{month([+-]?[0-9]+):([ymdhi-]+)\\}).*"), "month(%s, %s)");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{month\\.(start|end)\\}).*"), "month(0, %s, 0, 'yyyyMMdd')");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{month([+-]?[0-9]+)\\.(start|end)([+-]?[0-9]+):([ymdhi-]+)\\}).*"), "month(%s, %s, %s, %s)");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{week\\}).*"), "week()");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{week\\.(start|end)\\}).*"), "week(%s)");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{week\\.(start|end):([ymdhi-]+)\\}).*"), "week(%s, %s)");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{date\\}).*"), "date()");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{date([+-]?[0-9]+)\\}).*"), "date(%s)");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{hour\\}).*"), "hour()");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{hour([+-]?[0-9]+)\\}).*"), "hour(%s)");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{hour([+-]?[0-9]+):([ymdhi-]+)\\}).*"), "hour(%s, %s)");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{minute\\}).*"), "minute()");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{minute([+-]?[0-9]+)\\}).*"), "minute(%s)");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{minute([+-]?[0-9]+):([ymdhi-]+)\\}).*"), "minute(%s, %s)");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{quarter\\}).*"), "quarter()");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{quarter([+-]?[0-9]+)\\}).*"), "quarter(%s)");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{quarter\\.(start|end)\\}).*"), "quarter(%s, 0, 'yyyyMMdd')");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{quarter\\.(start|end)([+-]?[0-9]+):([ymdhi-]+)\\}).*"), "quarter(%s, %s, %s)");

        // format mappings
        DATE_FORMAT_MAPPINGS.put("y-m", MONTH_STRIKE_PATTERN);
        DATE_FORMAT_MAPPINGS.put("ym", MONTH_PATTERN);
        DATE_FORMAT_MAPPINGS.put("y-m-d", DAY_STRIKE_PATTERN);
        DATE_FORMAT_MAPPINGS.put("ymd", DAY_PATTERN);
        DATE_FORMAT_MAPPINGS.put("ymdh", HOUR_PATTERN);
        DATE_FORMAT_MAPPINGS.put("ymdhi", MINUTE_PATTERN);
    }

    public static String addYear(int number) {
        return String.valueOf(LocalDate.now().plusYears(number).getYear());
    }

    public static String addMonth(int number, String pattern) {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.MONTH, number);
        return DateFormatUtils.format(calendar.getTime(), pattern);
    }

    public static String addDate(int number, String pattern) {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, number);
        return DateFormatUtils.format(calendar.getTime(), pattern);
    }

    public static String addHour(int number) {
        return String.valueOf(LocalDateTime.now().plusHours(number).getHour());
    }

    public static String addHour(int number, String pattern) {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.HOUR, number);
        return DateFormatUtils.format(calendar.getTime(), pattern);
    }

    public static String addMinute(int number) {
        return String.valueOf(LocalDateTime.now().plusMinutes(number).getMinute());
    }

    public static String addMinute(int number, String pattern) {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.MINUTE, number);
        return DateFormatUtils.format(calendar.getTime(), pattern);
    }

    public static String weekOfYear() {
        Calendar calendar = Calendar.getInstance();
        return calendar.getWeekYear() + "" + calendar.get(Calendar.WEEK_OF_YEAR);
    }

    /**
     * Get first or end date of week, default set monday as week first day.
     */
    public static String weekDate(DateDirectionEnum dateDirectionEnum, String pattern) {
        Calendar calendar = Calendar.getInstance();
        // set monday as week first day
        calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
        if (dateDirectionEnum == DateDirectionEnum.END) {
            calendar.add(Calendar.WEEK_OF_MONTH, 1);
            calendar.add(Calendar.DAY_OF_WEEK, -1);
        }
        return DateFormatUtils.format(calendar.getTime(), pattern);
    }

    public static String quarterOfYear() {
        LocalDate localDate = LocalDate.now();
        int quarter = getQuarter(localDate.getMonthValue());
        return String.format("%d0%d", localDate.getYear(), quarter);
    }

    public static String addQuarter(int number) {
        LocalDate localDate = LocalDate.now().plusMonths(number * 3);
        int quarter = getQuarter(localDate.getMonthValue());
        return String.format("%d0%d", localDate.getYear(), quarter);
    }

    /**
     * Get the date with adding number from quarter start or end.
     */
    public static String quarterDate(DateDirectionEnum dateDirectionEnum, int number, String pattern) {
        LocalDate localDate = LocalDate.now();
        int month;
        int quarter = getQuarter(localDate.getMonthValue());
        if (dateDirectionEnum == DateDirectionEnum.START) {
            if (quarter == 1) {
                month = 1;
            } else if (quarter == 2) {
                month = 4;
            } else if (quarter == 3) {
                month = 7;
            } else {
                month = 10;
            }
        } else {
            if (quarter == 1) {
                month = 3;
            } else if (quarter == 2) {
                month = 6;
            } else if (quarter == 3) {
                month = 9;
            } else {
                month = 12;
            }
        }

        Calendar calendar = new GregorianCalendar(localDate.getYear(), month - 1, 1);
        if (dateDirectionEnum == DateDirectionEnum.START) {
            calendar.set(Calendar.DAY_OF_MONTH, 1 + number);
        } else {
            calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH) + number);
        }
        return DateFormatUtils.format(calendar.getTime(), pattern);
    }

    private static int getQuarter(int month) {
        return month % 3 == 0 ? month / 3 : month / 3 + 1;
    }
}
