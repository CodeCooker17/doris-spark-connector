package org.apache.doris.spark.etl;

import com.google.gson.annotations.SerializedName;

import java.io.Serializable;
import java.util.List;

public class EtlSchema implements Serializable {

    @SerializedName(value = "partitionInfo")
    public EtlPartitionInfo partitionInfo;
    @SerializedName(value = "columns")
    public List<EtlColumn> columns;

    public EtlSchema(){

    }

    public EtlSchema(EtlPartitionInfo partitionInfo, List<EtlColumn> columns) {
        this.partitionInfo = partitionInfo;
        this.columns = columns;
    }

    @Override
    public String toString() {
        return "EtlSchema{" +
                "partitionInfo=" + partitionInfo +
                ", columns=" + columns +
                '}';
    }




}
