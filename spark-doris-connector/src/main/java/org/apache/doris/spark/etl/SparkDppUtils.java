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

import org.apache.doris.spark.etl.EtlPartitionInfo.EtlPartition;
import org.apache.doris.spark.exception.DorisException;

import com.esotericsoftware.minlog.Log;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkDppUtils {
    private static final Logger LOG = LoggerFactory.getLogger(SparkDppUtils.class);

    private static Map<String, Integer> bucketKeyMap = new HashMap<>();

    public static JavaRDD<InternalRow> repartitionByBuckets(QueryExecution dataframe, EtlPartitionInfo partitionInfo,
            List<EtlColumn> columns, int bucketsNum, StructType dstTableSchema)
            throws Exception {

        JavaPairRDD<String, InternalRow> result;

        List<String> keyAndPartitionColumnNames = new ArrayList<>();
        List<String> valueColumnNames = new ArrayList<>();

        for (EtlColumn etlColumn : columns) {
            if (etlColumn.isKey) {
                keyAndPartitionColumnNames.add(etlColumn.columnName);
            } else {
                if (partitionInfo.partitionColumnRefs.contains(etlColumn.columnName)) {
                    keyAndPartitionColumnNames.add(etlColumn.columnName);
                }
                valueColumnNames.add(etlColumn.columnName);
            }
        }


        List<Integer> partitionKeyIndex = new ArrayList<>();
        List<Class> partitionKeySchema = new ArrayList<>();
        for (String key : partitionInfo.partitionColumnRefs) {
            for (EtlColumn column : columns) {
                if (column.columnName.equals(key)) {
                    partitionKeyIndex.add(keyAndPartitionColumnNames.indexOf(key));
                    partitionKeySchema.add(DppUtils.getClassFromColumn(column));
                    break;
                }
            }
        }

        List<DorisRangePartitioner.PartitionRangeKey> partitionRangeKeys
                = createPartitionRangeKeys(partitionInfo, partitionKeySchema);

        result = fillTupleWithPartitionColumn(
                dataframe,
                partitionInfo, partitionKeyIndex,
                partitionRangeKeys,
                keyAndPartitionColumnNames,
                dstTableSchema, bucketsNum);


        JavaPairRDD<String, InternalRow> listJavaPairRDD = result.partitionBy(
                new BucketPartitioner(bucketKeyMap, bucketsNum));

        return listJavaPairRDD.map(x -> x._2);
    }

    private static List<DorisRangePartitioner.PartitionRangeKey> createPartitionRangeKeys(
            EtlPartitionInfo partitionInfo, List<Class> partitionKeySchema) throws DorisException {
        List<DorisRangePartitioner.PartitionRangeKey> partitionRangeKeys = new ArrayList<>();

        for (EtlPartition partition : partitionInfo.partitions) {
            DorisRangePartitioner.PartitionRangeKey partitionRangeKey = new DorisRangePartitioner.PartitionRangeKey();
            List<Object> startKeyColumns = new ArrayList<>();
            for (int i = 0; i < partition.startKeys.size(); i++) {
                Object value = partition.startKeys.get(i);
                startKeyColumns.add(convertPartitionKey(value, partitionKeySchema.get(i)));
            }
            partitionRangeKey.startKeys = new DppColumns(startKeyColumns);
            if (!partition.isMaxPartition) {
                partitionRangeKey.isMaxPartition = false;
                List<Object> endKeyColumns = new ArrayList<>();
                for (int i = 0; i < partition.endKeys.size(); i++) {
                    Object value = partition.endKeys.get(i);
                    endKeyColumns.add(convertPartitionKey(value, partitionKeySchema.get(i)));
                }
                partitionRangeKey.endKeys = new DppColumns(endKeyColumns);
            } else {
                partitionRangeKey.isMaxPartition = true;
            }
            partitionRangeKeys.add(partitionRangeKey);
        }
        return partitionRangeKeys;
    }

    private static Object convertPartitionKey(Object srcValue, Class dstClass) throws DorisException {
        if (dstClass.equals(Float.class) || dstClass.equals(Double.class)) {
            return null;
        }
        if (srcValue instanceof Double) {
            if (dstClass.equals(Short.class)) {
                return ((Double) srcValue).shortValue();
            } else if (dstClass.equals(Integer.class)) {
                return ((Double) srcValue).intValue();
            } else if (dstClass.equals(Long.class)) {
                return ((Double) srcValue).longValue();
            } else if (dstClass.equals(BigInteger.class)) {
                // TODO(wb) gson will cast origin value to double by default
                // when the partition column is largeint, this will cause error data
                // need fix it thoroughly
                return new BigInteger(srcValue.toString());
            } else if (dstClass.equals(java.sql.Date.class) || dstClass.equals(java.util.Date.class)) {
                double srcValueDouble = (double) srcValue;
                return convertToJavaDate((int) srcValueDouble);
            } else if (dstClass.equals(java.sql.Timestamp.class)) {
                double srcValueDouble = (double) srcValue;
                return convertToJavaDatetime((long) srcValueDouble);
            } else {
                // dst type is string
                return srcValue.toString();
            }
        } else {
            LOG.warn("unsupport partition key:" + srcValue);
            throw new DorisException("unsupport partition key:" + srcValue);
        }
    }

    private static java.sql.Date convertToJavaDate(int originDate) {
        int day = originDate & 0x1f;
        originDate >>= 5;
        int month = originDate & 0x0f;
        originDate >>= 4;
        int year = originDate;
        return java.sql.Date.valueOf(String.format("%04d-%02d-%02d", year, month, day));
    }

    private static java.sql.Timestamp convertToJavaDatetime(long src) {
        String dateTimeStr = Long.valueOf(src).toString();
        if (dateTimeStr.length() != 14) {
            throw new RuntimeException("invalid input date format for SparkDpp");
        }

        String year = dateTimeStr.substring(0, 4);
        String month = dateTimeStr.substring(4, 6);
        String day = dateTimeStr.substring(6, 8);
        String hour = dateTimeStr.substring(8, 10);
        String min = dateTimeStr.substring(10, 12);
        String sec = dateTimeStr.substring(12, 14);

        return java.sql.Timestamp.valueOf(String.format("%s-%s-%s %s:%s:%s", year, month, day, hour, min, sec));
    }

    public static JavaPairRDD<String, InternalRow> fillTupleWithPartitionColumn(QueryExecution dataframe,
            EtlPartitionInfo partitionInfo, List<Integer> partitionKeyIndex,
            List<DorisRangePartitioner.PartitionRangeKey> partitionRangeKeys,
            List<String> keyAndPartitionColumnNames, StructType dstTableSchema, int bucketsNum) {
        List<String> distributeColumns = partitionInfo.distributionColumnRefs;
        Partitioner partitioner = new DorisRangePartitioner(partitionInfo, partitionKeyIndex, partitionRangeKeys);

        // use PairFlatMapFunction instead of PairMapFunction because the there will be
        // 0 or 1 output row for 1 input row
        JavaPairRDD<String, InternalRow> resultPairRDD = dataframe.toRdd().toJavaRDD().flatMapToPair(
                (PairFlatMapFunction<InternalRow, String, InternalRow>) row -> {
                    List<Tuple2<String, InternalRow>> result = new ArrayList<>();
                    List<Object> keyAndPartitionColumns = new ArrayList<>();
                    StructField[] fields = dstTableSchema.fields();
                    for (int i = 0; i < keyAndPartitionColumnNames.size(); i++) {
                        String columnName = keyAndPartitionColumnNames.get(i);
                        int columnIndex = dstTableSchema.fieldIndex(columnName);
                        if(columnIndex == -1){
                            throw new DorisException("hash value can not find column index.");
                        }
                        Object columnObject = row.get(columnIndex, fields[columnIndex].dataType());
                        keyAndPartitionColumns.add(columnObject);
                    }
                    DppColumns key = new DppColumns(keyAndPartitionColumns);
                    int pid = partitioner.getPartition(key);
                    // TODO(wb) support lagreint for hash
                    long hashValue = DppUtils.getHashValue(row, distributeColumns, dstTableSchema);
                    int finalBucketNumber = getRepartitionSize(partitionInfo.partitions.get(pid).bucketNum, bucketsNum);
                    int bucketId = (int) ((hashValue & 0xffffffff) % finalBucketNumber);
                    long partitionId = partitionInfo.partitions.get(pid).partitionId;
                    // bucketKey is partitionId_bucketId
                    String bucketKey = partitionId + "_" + bucketId;
                    result.add(new Tuple2<>(bucketKey, row));
                    return result.iterator();
                });

        initBucketKeyMap(partitionInfo, bucketsNum);
        // print to system.out for easy to find log info
        Log.info("print bucket key map: {} ", bucketKeyMap.toString());

        return resultPairRDD;
    }

    private static int getRepartitionSize(int bucketNumOfPartition, int resetBucketNum) {

        if(resetBucketNum == 0){
            return bucketNumOfPartition;
        }
        int base = resetBucketNum / bucketNumOfPartition;
        base = base == 0 ? 1 : base;

        return base * bucketNumOfPartition;
    }

    private static void initBucketKeyMap(EtlPartitionInfo partitionInfo, int bucketsNum) {
        LOG.info("Init bucket key map.");
        int reduceNum = 0;
        for (EtlPartition partition : partitionInfo.partitions) {
            int finalBucketNumber = bucketsNum == 0 ? partition.bucketNum : bucketsNum;
            for (int i = 0; i < finalBucketNumber; i++) {
                bucketKeyMap.put(partition.partitionId + "_" + i, reduceNum);
                reduceNum++;
            }
        }
    }
}
