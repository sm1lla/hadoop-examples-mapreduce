package com.opstty.job;

import com.opstty.IntIntWritable;
import com.opstty.mapper.DistrictMapper;
import com.opstty.mapper.DistrictTreesMapper;
import com.opstty.mapper.MaxTreesMapper;
import com.opstty.reducer.DistrictReducer;
import com.opstty.reducer.IntSumReducer;
import com.opstty.reducer.MaxTreesReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DistrictTreesCount {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: maxtrees <input path> <intermediate path> <output path>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "maxtrees1");
        job.setJarByClass(DistrictTreesCount.class);
        job.setMapperClass(DistrictTreesMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 2; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 2]));
        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "maxtrees2");
        job2.setJarByClass(DistrictTreesCount.class);
        job2.setMapperClass(MaxTreesMapper.class);
        job2.setCombinerClass(MaxTreesReducer.class);
        job2.setReducerClass(MaxTreesReducer.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(IntIntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(otherArgs[otherArgs.length - 2]));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
