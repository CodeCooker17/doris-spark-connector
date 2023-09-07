package org.apache.doris.spark.etl;

import org.apache.spark.Partitioner;

import java.util.Map;

public class BucketPartitioner extends Partitioner {
    private Map<String, Integer> bucketKeyMap;
    private int partitionNum;

    public BucketPartitioner(Map<String, Integer> bucketKeyMap, int partitionNum) {
        this.bucketKeyMap = bucketKeyMap;
        this.partitionNum = partitionNum;
    }

    @Override
    public int numPartitions() {
        return partitionNum;
    }

    @Override
    public int getPartition(Object key) {
        return bucketKeyMap.get(key.toString());
    }
}
