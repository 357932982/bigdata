package com.bigdata.mapreduce.proviceSortCombiner;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 指定本程序的jar包所在的本地路径
        job.setJarByClass(FlowCountDriver.class);

        // 指定本业务job要使用的mapper/reducer业务类
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountCombiner.class);

        job.setCombinerClass(FlowCountCombiner.class);



        // 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);



        // 指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);


        // 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 指定job输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        boolean res = job.waitForCompletion(true);
        System.out.println(res?0:1);
        System.exit(res?0:1);

    }

}
