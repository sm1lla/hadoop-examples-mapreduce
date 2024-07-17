package com.opstty.mapper;

import com.opstty.IntIntWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTreesMapper extends Mapper<Object, Text, NullWritable, IntIntWritable> {

    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] columns = line.split("\t");
        int num = Integer.parseInt(columns[1]);
        int district = Integer.parseInt(columns[0]);

        context.write(NullWritable.get(), new IntIntWritable(num, district));

    }

}
