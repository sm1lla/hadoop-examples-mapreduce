package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KindHeightMapper extends Mapper<Object, Text, Text, FloatWritable> {
    private boolean isFirstLine = true;

    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        if (isFirstLine) {
            isFirstLine = false; // Skip the first line
            return;
        }

        String line = value.toString();
        String[] columns = line.split(";");
        if (columns.length >= 7) {
            if(columns[6] == null || columns[6].isEmpty()) {
                return;
            }
            float height = Float.parseFloat(columns[6]);
            context.write(new Text(columns[2]), new FloatWritable(height));
        }
    }

}
