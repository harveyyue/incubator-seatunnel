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

import java.util.Map;

public class WeekFunction extends AbstractFunction {

    /**
     * Get current week of year
     * function like: week()=202251
     */
    @Override
    public AviatorObject call(Map<String, Object> env) {
        return new AviatorString(AviatorDateUtils.weekOfYear());
    }

    /**
     * Get first or end date of week, default set monday as week first day.
     * function like: week('start'), week('end')
     */
    public AviatorObject call(Map<String, Object> env, AviatorObject arg1) {
        return call(env, arg1, new AviatorString("yyyyMMdd"));
    }

    /**
     * function like: week('start', 'yyyyMMdd'), week('end', 'yyyy-MM-dd')
     */
    public AviatorObject call(Map<String, Object> env, AviatorObject arg1, AviatorObject arg2) {
        String direction = FunctionUtils.getStringValue(arg1, env);
        DateDirectionEnum dateDirectionEnum = DateDirectionEnum.getByName(direction);
        String pattern = FunctionUtils.getStringValue(arg2, env);
        return new AviatorString(AviatorDateUtils.weekDate(dateDirectionEnum, pattern));
    }

    @Override
    public String getName() {
        return "week";
    }
}
