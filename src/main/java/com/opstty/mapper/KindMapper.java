package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KindMapper extends Mapper<Object, Text, Text, IntWritable> {
    private boolean isFirstLine = true;
    private final static IntWritable one = new IntWritable(1);

    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        if (isFirstLine) {
            isFirstLine = false; // Skip the first line
            return;
        }

        String line = value.toString();
        String[] columns = line.split(";"); // Assuming columns are space-separated
        if (columns.length >= 3) {
            context.write(new Text(columns[2]), one);
        }
    }

}
