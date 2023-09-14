// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.spark.etl;

import com.google.gson.annotations.SerializedName;

import java.io.Serializable;

public class EtlColumn implements Serializable {
    @SerializedName(value = "columnName")
    public String columnName;
    @SerializedName(value = "columnType")
    public String columnType;
    @SerializedName(value = "isAllowNull")
    public boolean isAllowNull;
    @SerializedName(value = "isKey")
    public boolean isKey;
    @SerializedName(value = "aggregationType")
    public String aggregationType;
    @SerializedName(value = "defaultValue")
    public String defaultValue;
    @SerializedName(value = "stringLength")
    public int stringLength;
    @SerializedName(value = "precision")
    public int precision;
    @SerializedName(value = "scale")
    public int scale;
    @SerializedName(value = "defineExpr")
    public String defineExpr;

    // for unit test
    public EtlColumn() { }

    public EtlColumn(String columnName, String columnType, boolean isAllowNull, boolean isKey,
            String aggregationType, String defaultValue, int stringLength, int precision, int scale) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.isAllowNull = isAllowNull;
        this.isKey = isKey;
        this.aggregationType = aggregationType;
        this.defaultValue = defaultValue;
        this.stringLength = stringLength;
        this.precision = precision;
        this.scale = scale;
        this.defineExpr = null;
    }

    @Override
    public String toString() {
        return "EtlColumn{"
                + "columnName='" + columnName + '\''
                + ", columnType='" + columnType + '\''
                + ", isAllowNull=" + isAllowNull
                + ", isKey=" + isKey
                + ", aggregationType='" + aggregationType + '\''
                + ", defaultValue='" + defaultValue + '\''
                + ", stringLength=" + stringLength
                + ", precision=" + precision
                + ", scale=" + scale
                + ", defineExpr='" + defineExpr + '\''
                + '}';
    }
}