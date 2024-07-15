package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HeightSortMapper extends Mapper<Object, Text, FloatWritable, NullWritable> {
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
            context.write(new FloatWritable(height), NullWritable.get());
        }
    }

}
