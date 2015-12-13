package com.sela.bdhs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CatCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text category, Iterable<LongWritable> ones, Context context) throws IOException, InterruptedException {
        Long sum = 0L;
        for (LongWritable one : ones) {
            sum += one.get();
        }
        context.write(category, new LongWritable(sum));
    }
}
