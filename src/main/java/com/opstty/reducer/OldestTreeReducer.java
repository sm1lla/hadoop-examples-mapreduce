package com.opstty.reducer;

import com.opstty.IntIntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OldestTreeReducer extends Reducer<IntWritable, IntIntWritable, IntWritable, IntIntWritable> {
    private IntIntWritable result = new IntIntWritable();

    public void reduce(IntWritable key, Iterable<IntIntWritable> values, Context context)
            throws IOException, InterruptedException {
        int min = Integer.MAX_VALUE;
        int district = 0;
        for (IntIntWritable val : values) {
            int year = val.getInt1();
            if (year < min) {
                min = year;
                district = val.getInt2();
            }
        }
        result = new IntIntWritable(min, district);
        context.write(new IntWritable(district), result);
    }
}
