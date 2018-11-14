package com.bigdata.mapreduce.proviceSort;

import org.apache.hadoop.mapreduce.Reducer;

import javax.xml.soap.Text;
import java.io.IOException;

public class FlowCountReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(values.iterator().next(), key);
    }
}
