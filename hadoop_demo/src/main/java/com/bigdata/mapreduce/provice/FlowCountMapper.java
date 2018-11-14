package com.bigdata.mapreduce.provice;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private FlowBean flowBean = null;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 将一行内容转换成String
        String line = value.toString();
        // 切分字段
        String[] fields = line.split("\t");
        // 取出手机号
        String phomeNo = fields[1];
        // 取出上行下行流量
        long upFlow = Long.parseLong(fields[fields.length-3]);
        long dFlow = Long.parseLong(fields[fields.length-2]);
        long sumFlow = upFlow+dFlow;

        flowBean = FlowBean.builder().upFlow(upFlow).dFlow(dFlow).sumFlow(sumFlow).build();

        context.write(new Text(phomeNo), flowBean);


    }
}
