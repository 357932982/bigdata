package com.bigdata.mapreduce.proviceSortCombiner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountCombiner extends Reducer<Text, FlowBean, Text, FlowBean> {

    FlowBean flowBean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long sum_upFlow = 0;
        long sum_dFlow = 0;

        for (FlowBean item: values) {
            sum_upFlow += item.getUpFlow();
            sum_dFlow += item.getDFlow();
        }

        flowBean.set(sum_upFlow, sum_dFlow);

        context.write(key, flowBean);
    }
}
