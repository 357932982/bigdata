package com.bigdata.mapreduce.mapsice_join;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

public class MapSideJoin {

    public static class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

        HashMap<String, String> goods_Map = new HashMap<>();
        Text k = new Text();
        /**
         * setup方法是在maptask处理数据之前调用一次 可以用来做一些初始化工作
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {


            BufferedReader reader = new BufferedReader(new FileReader("/home/media/Ubuntu/hadoop_learn/rjoin/input/goods"));
            String line;
            while (StringUtils.isNotEmpty(line = reader.readLine())) {
                String[] fields = line.split(" ");
                goods_Map.put(fields[0], fields[1]);
            }
            reader.close();
        }

        // 由于已经持有完整的产品信息表，所以在map方法中就能实现join逻辑了

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String order_line = value.toString();
            String[] fields = order_line.split(" ");

            String field = fields[2];
            String goods_name = goods_Map.get(field);
            k.set(order_line + "\t" + goods_name);
            context.write(k, NullWritable.get());

        }

    }


    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


        job.setJarByClass(MapSideJoin.class);

        job.setMapperClass(MapSideJoinMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("/home/media/Ubuntu/hadoop_learn/rjoin/mapside_input"));
        FileOutputFormat.setOutputPath(job, new Path("/home/media/Ubuntu/hadoop_learn/rjoin/mapside_output"));

        // 指定需要缓存一个文件到所有的maptask运行节点工作目录
        /* job.addArchiveToClassPath(archive); */// 缓存jar包到task运行节点的classpath中
        /* job.addFileToClassPath(file); */// 缓存普通文件到task运行节点的classpath中
        /* job.addCacheArchive(uri); */// 缓存压缩包文件到task运行节点的工作目录
        /* job.addCacheFile(uri) */// 缓存普通文件到task运行节点的工作目录

        // 将产品表文件缓存到task工作节点的工作目录中去
        job.addCacheFile(new URI("file:///home/media/Ubuntu/hadoop_learn/rjoin/mapside_goods"));

        //map端join的逻辑不需要reduce阶段，设置reducetask数量为0
        job.setNumReduceTasks(0);

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);

    }
}
