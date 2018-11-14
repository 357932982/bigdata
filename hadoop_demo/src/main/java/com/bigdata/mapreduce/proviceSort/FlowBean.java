package com.bigdata.mapreduce.proviceSort;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 普通流量统计功能
 */
@Getter
@Setter
public class FlowBean implements WritableComparable<FlowBean> {

    private long upFlow;
    private long dFlow;
    private long sumFlow;



    public void set(long upFlow, long dFlow) {
        this.upFlow = upFlow;
        this.dFlow = dFlow;
        this.sumFlow = upFlow+dFlow;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + dFlow + "\t" + sumFlow ;
    }

    /**
     * 序列化方法
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {

        out.writeLong(upFlow);
        out.writeLong(dFlow);
        out.writeLong(sumFlow);
    }

    /**
     * 反序列化
     * 顺序和序列化顺序一致
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {

        upFlow = in.readLong();
        dFlow = in.readLong();
        sumFlow = in.readLong();
    }


    @Override
    public int compareTo(FlowBean o) {
        return this.getSumFlow() > o.getSumFlow()?-1:1;
    }
}
