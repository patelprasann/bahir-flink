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

package org.apache.flink.streaming.connectors.timestreamdb;

import software.amazon.awssdk.services.timestreamwrite.model.MeasureValueType;

import java.util.HashMap;
import java.util.Map;

public class TimestreamPoint {
    private String measureName;
    private MeasureValueType measureValueType;
    private String measureValue;
    private long time;
    private String timeUnit;
    private Map<String, String> dimensions;

    public TimestreamPoint() {
        this.dimensions = new HashMap<>();
    }

    public TimestreamPoint(long time,
                           Map<String, String> dimensions,
                           String measureName,
                           String measureValueType,
                           String measureValue) {
        this.time = time;
        this.dimensions = dimensions;
        this.measureName = measureName;
        this.measureValueType = MeasureValueType.fromValue(measureValueType.toUpperCase());
        this.measureValue = measureValue;
    }

    public TimestreamPoint(long time,
                           String measureName,
                           String measureValueType,
                           String measureValue) {
        this.time = time;
        this.dimensions = new HashMap<>();
        this.measureName = measureName;
        this.measureValueType = MeasureValueType.fromValue(measureValueType.toUpperCase());
        this.measureValue = measureValue;
    }

    public String getMeasureName() {
        return measureName;
    }

    public void setMeasureName(String measureName) {
        this.measureName = measureName;
    }

    public String getMeasureValue() {
        return measureValue;
    }

    public void setMeasureValue(String measureValue) {
        this.measureValue = measureValue;
    }

    public MeasureValueType getMeasureValueType() {
        return measureValueType;
    }

    public void setMeasureValueType(MeasureValueType measureValueType) {
        this.measureValueType = measureValueType;
    }

    public void setMeasureValueType(String measureValueType) {
        this.measureValueType = MeasureValueType.fromValue(measureValueType.toUpperCase());
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(String timeUnit) {
        this.timeUnit = timeUnit;
    }

    public Map<String, String> getDimensions() {
        return dimensions;
    }

    public void setDimensions(Map<String, String> dims) {
        this.dimensions = dims;
    }

    public void addDimension(String dimensionName, String dimensionValue) {
        dimensions.put(dimensionName, dimensionValue);
    }
}
