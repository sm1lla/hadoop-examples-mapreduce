package com.opstty.mapper;

import com.opstty.IntIntWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OldestTreeMapper extends Mapper<Object, Text, IntWritable, IntIntWritable> {
    private boolean isFirstLine = true;

    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        if (isFirstLine) {
            isFirstLine = false; // Skip the first line
            return;
        }

        String line = value.toString();
        String[] columns = line.split(";");
        if (columns.length >= 6) {
            if(columns[5] == null || columns[5].isEmpty()) {
                return;
            }
            int year = Integer.parseInt(columns[5]);
            int district = Integer.parseInt(columns[1]);

            context.write(new IntWritable(1), new IntIntWritable(year, district));
        }
    }

}
