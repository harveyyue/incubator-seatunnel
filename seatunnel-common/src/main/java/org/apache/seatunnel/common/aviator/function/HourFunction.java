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
import com.googlecode.aviator.runtime.type.AviatorObject;
import com.googlecode.aviator.runtime.type.AviatorString;

import java.util.Map;

public class HourFunction extends AbstractFunction {

    /**
     * function like: hour()
     */
    @Override
    public AviatorObject call(Map<String, Object> env) {
        return new AviatorString(AviatorDateUtils.currentHour());
    }

    /**
     * function like: hour(-1)
     */
    @Override
    public AviatorObject call(Map<String, Object> env, AviatorObject arg1) {
        Number plusHour = FunctionUtils.getNumberValue(arg1, env);
        return new AviatorString(AviatorDateUtils.addHour(plusHour.intValue()));
    }

    /**
     * function like: hour(-1, 'yyyy-MM-dd')
     */
    @Override
    public AviatorObject call(Map<String, Object> env, AviatorObject arg1, AviatorObject arg2) {
        Number plusHour = FunctionUtils.getNumberValue(arg1, env);
        String pattern = FunctionUtils.getStringValue(arg2, env);
        return new AviatorString(AviatorDateUtils.addHour(plusHour.intValue(), pattern));
    }

    @Override
    public String getName() {
        return "hour";
    }
}
