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

package org.apache.doris.spark.util;

import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

import java.sql.Timestamp;
import java.util.Arrays;

public class DataUtil {

    public static final String NULL_VALUE = "\\N";

    public static Object handleColumnValue(Object value) {

        if (value == null) {
            return NULL_VALUE;
        }

        if (value instanceof Timestamp) {
            return value.toString();
        }

        if (value instanceof WrappedArray) {

            Object[] arr = JavaConversions.seqAsJavaList((WrappedArray) value).toArray();
            return Arrays.toString(arr);
        }

        return value;

    }

    public static Object handleColumnValueAddQuotes(Object value) {

        if (value == null) {
            return NULL_VALUE;
        }

        if (value instanceof Timestamp) {
            return value.toString();
        }

        if (value instanceof WrappedArray) {

            Object[] arr = JavaConversions.seqAsJavaList((WrappedArray) value).toArray();
            return Arrays.toString(arr);
        }

        return "\"" + value + "\"";
    }
}
