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

import org.apache.doris.spark.exception.DorisException;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Date;
import java.util.List;
import java.util.zip.CRC32;

public class DppUtils {

    public static Class getClassFromColumn(EtlColumn column) throws Exception {
        switch (column.columnType) {
            case "BOOLEAN":
                return Boolean.class;
            case "TINYINT":
            case "SMALLINT":
                return Short.class;
            case "INT":
                return Integer.class;
            case "DATETIME":
            case "DATETIMEV2":
                return java.sql.Timestamp.class;
            case "BIGINT":
                return Long.class;
            case "LARGEINT":
                throw new Exception("LARGEINT is not supported now");
            case "FLOAT":
                return Float.class;
            case "DOUBLE":
                return Double.class;
            case "DATE":
            case "DATEV2":
                return Date.class;
            case "HLL":
            case "CHAR":
            case "VARCHAR":
            case "STRING":
            case "TEXT":
            case "BITMAP":
            case "OBJECT":
                return String.class;
            case "DECIMALV2":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128":
                return BigDecimal.valueOf(column.precision, column.scale).getClass();
            default:
                return String.class;
        }
    }

    public static ByteBuffer getHashValue(Object o, DataType type) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        if (o == null) {
            buffer.putInt(0);
            return buffer;
        }
        if (type.equals(DataTypes.ByteType)) {
            buffer.put((byte) o);
        } else if (type.equals(DataTypes.ShortType)) {
            buffer.putShort((Short) o);
        } else if (type.equals(DataTypes.IntegerType)) {
            buffer.putInt((Integer) o);
        } else if (type.equals(DataTypes.LongType)) {
            buffer.putLong((Long) o);
        } else if (type.equals(DataTypes.StringType)) {
            try {
                String str = String.valueOf(o);
                buffer = ByteBuffer.wrap(str.getBytes("UTF-8"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else if (type.equals(DataTypes.BooleanType)) {
            Boolean b = (Boolean) o;
            byte value = (byte) (b ? 1 : 0);
            buffer.put(value);
        }
        // do not flip buffer when the buffer was created by wrap()
        if (!type.equals(DataTypes.StringType)) {
            buffer.flip();
        }
        return buffer;
    }

    public static long getHashValue(InternalRow row, List<String> distributeColumns, StructType dstTableSchema)
            throws DorisException {
        CRC32 hashValue = new CRC32();
        StructField[] fields = dstTableSchema.fields();
        for (String distColumn : distributeColumns) {
            int columnIndex = dstTableSchema.fieldIndex(distColumn);
            if(columnIndex == -1){
                throw new DorisException("hash value can not find column.");
            }
            Object columnObject = row.get(columnIndex, fields[columnIndex].dataType());
            ByteBuffer buffer = getHashValue(columnObject, dstTableSchema.apply(distColumn).dataType());
            hashValue.update(buffer.array(), 0, buffer.limit());
        }
        return hashValue.getValue();
    }

}
