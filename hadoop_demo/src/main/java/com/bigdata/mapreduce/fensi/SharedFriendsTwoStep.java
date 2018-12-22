package com.bigdata.mapreduce.fensi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;

public class SharedFriendsTwoStep {

    static class ShatedFriendsStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] friends_persons = line.split("\t");

            String friend = friends_persons[0];
            String[] person = friends_persons[1].split(",");

            Arrays.sort(person);

            for (int i = 0; i < person.length-1; i++) {
                for (int j = i+1; j < person.length; j++) {
                    context.write(new Text(person[i]+"-"+person[j]), new Text(friend));
                }
            }
        }
    }

    static class SharedFriendsStepTwoReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text person_person, Iterable<Text> friends, Context context) throws IOException, InterruptedException {

            StringBuilder sb = new StringBuilder();
            for (Text friend:friends) {
                sb.append(friend).append(" ");
            }
            context.write(person_person, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(SharedFriendsTwoStep.class);

        job.setMapperClass(ShatedFriendsStepTwoMapper.class);
        job.setReducerClass(SharedFriendsStepTwoReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("/home/media/Ubuntu/hadoop_learn/friends/output/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("/home/media/Ubuntu/hadoop_learn/friends/output/output"));

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
