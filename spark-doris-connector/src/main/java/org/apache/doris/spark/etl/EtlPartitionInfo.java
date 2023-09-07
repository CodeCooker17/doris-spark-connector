package org.apache.doris.spark.etl;


import com.google.gson.annotations.SerializedName;
import jdk.nashorn.internal.objects.annotations.Getter;
import jdk.nashorn.internal.objects.annotations.Setter;

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