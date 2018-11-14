package com.bigdata.mapreduce.proviceSortCombiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    FlowBean flowBean = new FlowBean();
    Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] line = value.toString().split("\t");

        String phoneNo = line[1];

        long upFlow = Long.parseLong(line[line.length-3]);
        long dFlow = Long.parseLong(line[line.length-2]);


        flowBean.set(upFlow, dFlow);
        text.set(phoneNo);

        context.write(text, flowBean);
    }
}
