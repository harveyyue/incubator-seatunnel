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

import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.googlecode.aviator.runtime.function.FunctionUtils;
import com.googlecode.aviator.runtime.type.AviatorLong;
import com.googlecode.aviator.runtime.type.AviatorObject;
import com.googlecode.aviator.runtime.type.AviatorString;

import java.util.Map;

public class DateFunction extends AbstractFunction {

    /**
     * function like: date()
     */
    @Override
    public AviatorObject call(Map<String, Object> env) {
        return call(env, AviatorLong.valueOf(0), new AviatorString("yyyy-MM-dd"));
    }

    /**
     * function like: date(1), date(-1)
     */
    @Override
    public AviatorObject call(Map<String, Object> env, AviatorObject arg1) {
        return call(env, arg1, new AviatorString("yyyy-MM-dd"));
    }

    /**
     * function like: date(1, 'yyyyMMdd')
     */
    @Override
    public AviatorObject call(Map<String, Object> env, AviatorObject arg1, AviatorObject arg2) {
        Number plusDate = FunctionUtils.getNumberValue(arg1, env);
        String pattern = FunctionUtils.getStringValue(arg2, env);
        return new AviatorString(AviatorDateUtils.addDate(plusDate.intValue(), pattern));
    }

    @Override
    public String getName() {
        return "date";
    }
}
