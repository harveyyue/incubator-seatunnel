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

@SuppressWarnings("MagicNumber")
public class QuarterFunction extends AbstractFunction {

    /**
     * function like: quarter()
     */
    @Override
    public AviatorObject call(Map<String, Object> env) {
        return new AviatorString(AviatorDateUtils.quarterOfYear());
    }

    /**
     * function like: quarter(1), quarter(-1)
     */
    @Override
    public AviatorObject call(Map<String, Object> env, AviatorObject arg1) {
        Number plusQuarter = FunctionUtils.getNumberValue(arg1, env);
        return new AviatorString(AviatorDateUtils.addQuarter(plusQuarter.intValue()));
    }

    /**
     * function like: quarter('start', 0, 'yyyyMMdd'), quarter('start', 1, 'yyyy-MM-dd'), quarter('end', -2, 'yyyy-MM-dd')
     */
    @Override
    public AviatorObject call(Map<String, Object> env, AviatorObject arg1, AviatorObject arg2, AviatorObject arg3) {
        String direction = FunctionUtils.getStringValue(arg1, env);
        DateDirectionEnum dateDirectionEnum = DateDirectionEnum.getByName(direction);
        Number plusDay = FunctionUtils.getNumberValue(arg2, env);
        String pattern = FunctionUtils.getStringValue(arg3, env);
        return new AviatorString(AviatorDateUtils.quarterDate(dateDirectionEnum, plusDay.intValue(), pattern));
    }

    @Override
    public String getName() {
        return "quarter";
    }

    private int getQuarter(int month) {
        return month % 3 == 0 ? month / 3 : month / 3 + 1;
    }
}
