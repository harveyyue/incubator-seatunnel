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

package org.apache.seatunnel.common.aviator.function;

import org.apache.seatunnel.common.aviator.AviatorDateUtils;
import org.apache.seatunnel.common.aviator.DateDirectionEnum;

import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.googlecode.aviator.runtime.function.FunctionUtils;
import com.googlecode.aviator.runtime.type.AviatorObject;
import com.googlecode.aviator.runtime.type.AviatorString;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

public class MonthFunction extends AbstractFunction {

    /**
     * month()
     */
    @Override
    public AviatorObject call(Map<String, Object> env) {
        String month = DateFormatUtils.format(new Date(), "yyyyMM");
        return new AviatorString(month);
    }

    /**
     * month(-1)
     */
    @Override
    public AviatorObject call(Map<String, Object> env, AviatorObject arg1) {
        return call(env, arg1, new AviatorString("yyyyMM"));
    }

    /**
     * month(1, 'yyyyMM')
     */
    @Override
    public AviatorObject call(Map<String, Object> env, AviatorObject arg1, AviatorObject arg2) {
        Number plusValue = FunctionUtils.getNumberValue(arg1, env);
        String pattern = FunctionUtils.getStringValue(arg2, env);
        return new AviatorString(AviatorDateUtils.addMonth(plusValue.intValue(), pattern));
    }

    /**
     * month(0, 'start', 0, 'yyyy-MM-dd'): 2022-12-01
     * month(0, 'end', 0, 'yyyy-MM-dd')  : 2022-12-31
     * month(-1, 'end', -2, 'yyyy-MM-dd'): 2022-11-28
     */
    public AviatorObject call(Map<String, Object> env, AviatorObject arg1, AviatorObject arg2, AviatorObject arg3, AviatorObject arg4) {
        Number plusMonth = FunctionUtils.getNumberValue(arg1, env);
        Number plusDay = FunctionUtils.getNumberValue(arg3, env);

        String direction = FunctionUtils.getStringValue(arg2, env);
        DateDirectionEnum dateDirectionEnum = DateDirectionEnum.getByName(direction);
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.MONTH, plusMonth.intValue());
        if (dateDirectionEnum == DateDirectionEnum.START) {
            calendar.set(Calendar.DAY_OF_MONTH, 1 + plusDay.intValue());
        } else {
            calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH) + plusDay.intValue());
        }
        String month = DateFormatUtils.format(calendar.getTime(), FunctionUtils.getStringValue(arg4, env));
        return new AviatorString(month);
    }

    @Override
    public String getName() {
        return "month";
    }
}
