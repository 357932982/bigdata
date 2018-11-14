package com.bigdata.mapreduce.reduce_join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class ReduceSideJoin {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[] {"/home/media/Ubuntu/hadoop_learn/rjoin/input", "/home/media/Ubuntu/hadoop_learn/rjoin/output"};
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(RjoinMapper.class);
        job.setReducerClass(RJoinReducer.class);

        // 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);

        // 指定最终输出的数据的kv类型
        job.setOutputKeyClass(InfoBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 指定job的输入原始文件所在目录
        // 指定job的输出结果所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        /* job.submit(); */

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }

}

class RjoinMapper extends Mapper<LongWritable, Text, Text, InfoBean>{

    InfoBean bean = new InfoBean();
    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split(" ");
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        String name = inputSplit.getPath().getName();

        // 通过pid判断是哪种数据
        String pid = "";
        if (name.startsWith("order")){
            pid = fields[2];
            bean.set(Integer.parseInt(fields[0]), fields[1], pid, Integer.parseInt(fields[3]), "", 0, 0, "0");
        } else {
            pid = fields[0];
            bean.set(0, "", pid, 0, fields[1], Integer.parseInt(fields[2]) , Integer.parseInt(fields[3]), "1");
        }
        k.set(pid);
        context.write(k, bean);

    }
}

class RJoinReducer extends Reducer<Text, InfoBean, InfoBean, NullWritable>{

    @Override
    protected void reduce(Text key, Iterable<InfoBean> beans, Context context) throws IOException, InterruptedException {

        // 因为一个pid只对应一个商品，所以商品对象只new一次
        ArrayList<InfoBean> orderBeans = new ArrayList<InfoBean>();
        InfoBean goods_Bean = new InfoBean();
        InfoBean order_Bean = null;

        for (InfoBean bean : beans){
            if ("1".equals(bean.getFlag())){
                try {
                    BeanUtils.copyProperties(goods_Bean, bean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            } else {
                order_Bean = new InfoBean();
                try {
                    BeanUtils.copyProperties(order_Bean, bean);
                    orderBeans.add(order_Bean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        // 拼接成两类数据形成最终的结果
        for (InfoBean bean : orderBeans) {
            bean.setPname(goods_Bean.getPname());
            bean.setCategory_id(goods_Bean.getCategory_id());
            bean.setPrice(goods_Bean.getPrice());

            context.write(bean, NullWritable.get());
        }
    }
}
