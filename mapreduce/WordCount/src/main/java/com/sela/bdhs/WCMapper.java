package com.sela.bdhs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final LongWritable one = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString().toLowerCase();
        StringTokenizer tokenizer = new StringTokenizer(row);
        Text word = new Text();
        while(tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, one);
        }

    }
}

