package com.opstty.reducer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HeightSortReducer extends Reducer<NullWritable, NullWritable, FloatWritable, NullWritable> {

    public void reduce(FloatWritable key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {

        context.write(key, NullWritable.get());
    }
}
