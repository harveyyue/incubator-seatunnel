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
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{year\\}).*", Pattern.DOTALL), "year()");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{year([+-]?[0-9]+)\\}).*", Pattern.DOTALL), "year(%s)");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{month\\}).*", Pattern.DOTALL), "month()");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{month([+-]?[0-9]+)\\}).*", Pattern.DOTALL), "month(%s)");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{month([+-]?[0-9]+):([ymdhi-]+)\\}).*", Pattern.DOTALL), "month(%s, %s)");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{month\\.(start|end)\\}).*", Pattern.DOTALL), "month(0, %s, 0, 'yyyyMMdd')");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{month([+-]?[0-9]+)\\.(start|end)([+-]?[0-9]+):([ymdhi-]+)\\}).*", Pattern.DOTALL), "month(%s, %s, %s, %s)");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{week\\}).*", Pattern.DOTALL), "week()");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{week\\.(start|end)\\}).*", Pattern.DOTALL), "week(%s)");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{week\\.(start|end):([ymdhi-]+)\\}).*", Pattern.DOTALL), "week(%s, %s)");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{date\\}).*", Pattern.DOTALL), "date()");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{date([+-]?[0-9]+)\\}).*", Pattern.DOTALL), "date(%s)");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{hour\\}).*", Pattern.DOTALL), "hour()");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{hour([+-]?[0-9]+)\\}).*", Pattern.DOTALL), "hour(%s)");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{hour([+-]?[0-9]+):([ymdhi-]+)\\}).*", Pattern.DOTALL), "hour(%s, %s)");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{minute\\}).*", Pattern.DOTALL), "minute()");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{minute([+-]?[0-9]+)\\}).*", Pattern.DOTALL), "minute(%s)");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{minute([+-]?[0-9]+):([ymdhi-]+)\\}).*", Pattern.DOTALL), "minute(%s, %s)");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{quarter\\}).*", Pattern.DOTALL), "quarter()");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{quarter([+-]?[0-9]+)\\}).*", Pattern.DOTALL), "quarter(%s)");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{quarter\\.(start|end)\\}).*", Pattern.DOTALL), "quarter(%s, 0, 'yyyyMMdd')");
        DATE_FUNCTION_PATTERNS.put(Pattern.compile(".*(\\$\\{quarter\\.(start|end)([+-]?[0-9]+):([ymdhi-]+)\\}).*", Pattern.DOTALL), "quarter(%s, %s, %s)");

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

    public static String currentHour() {
        return String.format("%02d", LocalDateTime.now().getHour());
    }

    public static String addHour(int number) {
        return String.format("%02d", LocalDateTime.now().plusHours(number).getHour());
    }

    public static String addHour(int number, String pattern) {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.HOUR, number);
        return DateFormatUtils.format(calendar.getTime(), pattern);
    }

    public static String currentMinute() {
        return String.format("%02d", LocalDateTime.now().getMinute());
    }

    public static String addMinute(int number) {
        return String.format("%02d", LocalDateTime.now().plusMinutes(number).getMinute());
    }

    public static String addMinute(int number, String pattern) {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.MINUTE, number);
        return DateFormatUtils.format(calendar.getTime(), pattern);
    }

    public static String weekOfYear() {
        Calendar calendar = Calendar.getInstance();
        return calendar.getWeekYear() + String.format("%02d", calendar.get(Calendar.WEEK_OF_YEAR));
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
