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

import java.io.Serializable;
import java.util.List;

public class EtlPartitionInfo implements Serializable {
    public String partitionType;

    public List<String> partitionColumnRefs;
    public List<String> distributionColumnRefs;
    public List<EtlPartition> partitions;

    public EtlPartitionInfo() {
    }
    public EtlPartitionInfo(String partitionType, List<String> partitionColumnRefs,
            List<String> distributionColumnRefs, List<EtlPartition> etlPartitions) {
        this.partitionType = partitionType;
        this.partitionColumnRefs = partitionColumnRefs;
        this.distributionColumnRefs = distributionColumnRefs;
        this.partitions = etlPartitions;
    }

    @Override
    public String toString() {
        return "EtlPartitionInfo{"
                + "partitionType='" + partitionType + '\''
                + ", partitionColumnRefs=" + partitionColumnRefs
                + ", distributionColumnRefs=" + distributionColumnRefs
                + ", partitions=" + partitions
                + '}';
    }

    public static class EtlPartition implements Serializable {
        public long partitionId;
        public List<Object> startKeys;
        public List<Object> endKeys;
        public boolean isMaxPartition;
        public int bucketNum;

        public EtlPartition(){
        }

        public EtlPartition(long partitionId, List<Object> startKeys, List<Object> endKeys,
                boolean isMaxPartition, int bucketNum) {
            this.partitionId = partitionId;
            this.startKeys = startKeys;
            this.endKeys = endKeys;
            this.isMaxPartition = isMaxPartition;
            this.bucketNum = bucketNum;
        }


        @Override
        public String toString() {
            return "EtlPartition{"
                    + "partitionId=" + partitionId
                    + ", startKeys=" + startKeys
                    + ", endKeys=" + endKeys
                    + ", isMaxPartition=" + isMaxPartition
                    + ", bucketNum=" + bucketNum
                    + '}';
        }
    }
}