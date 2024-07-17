package com.opstty.reducer;

import com.opstty.IntIntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTreesReducer extends Reducer<NullWritable, IntIntWritable, NullWritable, IntIntWritable> {
    private IntIntWritable result = new IntIntWritable();

    public void reduce(NullWritable key, Iterable<IntIntWritable> values, Context context)
            throws IOException, InterruptedException {
        int max = Integer.MIN_VALUE;
        int district = 0;
        for (IntIntWritable val : values) {
            int num = val.getInt1();
            if (num > max) {
                max = num;
                district = val.getInt2();
            }
        }
        result = new IntIntWritable(max, district);
        context.write(NullWritable.get(), result);
    }
}
