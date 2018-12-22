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

public class SharedFriendsOneStep {

    static class ShareFriendsStepOneMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] person_friends = line.split(":");
            String person = person_friends[0];
            String friends = person_friends[1];

            for (String friend : friends.split(",")) {
                context.write(new Text(friend), new Text(person));
            }
        }
    }

    static class ShareFriendsStepOneReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text friend, Iterable<Text> persons, Context context) throws IOException, InterruptedException {

            StringBuilder sb = new StringBuilder();

            for (Text person : persons) {
                sb.append(person).append(",");
            }

            context.write(friend, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(SharedFriendsOneStep.class);

        job.setMapperClass(ShareFriendsStepOneMapper.class);
        job.setReducerClass(ShareFriendsStepOneReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("/home/media/Ubuntu/hadoop_learn/friends/input"));
        FileOutputFormat.setOutputPath(job, new Path("/home/media/Ubuntu/hadoop_learn/friends/output"));

        boolean res = job.waitForCompletion(true);

        System.exit(res ? 0 : 1);


    }
}
