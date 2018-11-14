package com.bigdata.mapreduce.proviceSort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    FlowBean flowBean = new FlowBean();
    Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 读取一行转换成String
        String line = value.toString();
        // 将数据切分
        String[] fields = line.split("\t");
        // 取出手机号
        String phoneNo = fields[1];

        long upFlow = Long.parseLong(fields[fields.length-3]);
        long dFlow = Long.parseLong(fields[fields.length-2]);

        flowBean.set(upFlow, dFlow);
        text.set(phoneNo);

        context.write(flowBean, text);

    }
}
