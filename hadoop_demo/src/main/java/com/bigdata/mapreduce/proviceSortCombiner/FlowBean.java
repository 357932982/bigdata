package com.bigdata.mapreduce.proviceSortCombiner;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.Tolerate;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@Builder
public class FlowBean implements WritableComparable<FlowBean> {

    private long upFlow;
    private long dFlow;
    private long sumFlow;

    @Tolerate
    public FlowBean(){

    }


    public void set(long upFlow, long dFlow) {
        this.upFlow = upFlow;
        this.dFlow = dFlow;
        this.sumFlow = upFlow+dFlow;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + dFlow + "\t" + sumFlow ;
    }

    @Override
    public int compareTo(FlowBean o) {
        return this.getSumFlow() > o.getSumFlow()?-1:1;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeLong(upFlow);
        out.writeLong(dFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        upFlow = in.readLong();
        dFlow = in.readLong();
        sumFlow = in.readLong();
    }
}
