package com.opstty.mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpeciesMapper extends Mapper<Object, Text, Text, NullWritable> {
    private boolean isFirstLine = true;

    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        if (isFirstLine) {
            isFirstLine = false; // Skip the first line
            return;
        }

        String line = value.toString();
        String[] columns = line.split(";"); // Assuming columns are space-separated
        if (columns.length >= 4) {
            context.write(new Text(columns[3]), NullWritable.get());
        }
    }

}
