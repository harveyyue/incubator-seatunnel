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

import java.time.LocalDateTime;
import java.util.Map;

public class YearFunction extends AbstractFunction {

    /**
     * function like: year()
     */
    @Override
    public AviatorObject call(Map<String, Object> env) {
        return new AviatorString(String.valueOf(LocalDateTime.now().getYear()));
    }

    /**
     * function like: year(1), year(-1)
     */
    @Override
    public AviatorObject call(Map<String, Object> env, AviatorObject arg1) {
        Number input = FunctionUtils.getNumberValue(arg1, env);
        return new AviatorString(AviatorDateUtils.addYear(input.intValue()));
    }

    @Override
    public String getName() {
        return "year";
    }
}
