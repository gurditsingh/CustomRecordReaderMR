package com.mapreduce;


import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FixedLenghtDemo {

    public static class mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        @Override
        public void map(LongWritable key,Text value,Context context){
            String line=value.toString();

                try {
                    context.write(new Text(line), new IntWritable(1));
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
    }

    public static void main(String arg[]) throws Exception{

        JobConf jobConf=new JobConf();
        jobConf.setInt(FixedLengthInputFormat.FIXED_RECORD_LENGTH, 10);

        Job job=new Job(jobConf);
        job.setJarByClass(FixedLenghtDemo.class);
        job.setJobName("Fixed Lenght");

        job.setInputFormatClass(FixedLengthInputFormat.class);

        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(arg[0]));
        FileOutputFormat.setOutputPath(job, new Path(arg[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(mapper.class);

        job.waitForCompletion(true);

    }

}
