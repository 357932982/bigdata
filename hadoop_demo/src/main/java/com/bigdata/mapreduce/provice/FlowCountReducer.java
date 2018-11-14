package com.bigdata.mapreduce.provice;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean flowBean = null;

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long sum_upFlow = 0;
        long sum_dFlow = 0;

        // 遍历所有的bean，将其中的上行流量，下行流量分别累加
        for (FlowBean bean : values) {
            sum_upFlow += bean.getUpFlow();
            sum_dFlow += bean.getDFlow();
        }
        flowBean = FlowBean.builder().upFlow(sum_upFlow).dFlow(sum_dFlow).sumFlow(sum_upFlow+sum_dFlow).build();


        context.write(key, flowBean);
    }
}
