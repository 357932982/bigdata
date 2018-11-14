package com.bigdata.mapreduce.provice;

import lombok.*;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 普通流量统计功能
 */
//@Data
@Getter
@Setter
@ToString
@Builder
public class FlowBean implements Writable {

    private long upFlow;
    private long dFlow;
    private long sumFlow;

    // 序列化和反序列化需要用到无参构造器
    public FlowBean() {
    }


    public FlowBean(long upFlow, long dFlow, long sumFlow) {
        this.upFlow = upFlow;
        this.dFlow = dFlow;
        this.sumFlow = sumFlow;
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


}
