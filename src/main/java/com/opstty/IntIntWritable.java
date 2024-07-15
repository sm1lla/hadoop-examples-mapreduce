package com.opstty;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class IntIntWritable implements Writable {
    private IntWritable value1;
    private IntWritable value2;

    public IntIntWritable() {
        value1 = new IntWritable();
        value2 = new IntWritable();
    }

    public IntIntWritable(int value1, int value2) {
        this.value1 = new IntWritable(value1);
        this.value2 = new IntWritable(value2);
    }

    public int getInt1() {
        return value1.get();
    }

    public int getInt2() {
        return value2.get();
    }

    @Override
    public String toString() {
        return "District: " + value2.toString();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value1.readFields(in);
        value2.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        value1.write(out);
        value2.write(out);
    }
}