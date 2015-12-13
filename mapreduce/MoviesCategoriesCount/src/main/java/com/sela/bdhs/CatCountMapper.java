package com.sela.bdhs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CatCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final LongWritable one = new LongWritable(1);

    @Override
    protected void map(LongWritable offset, Text row, Context context) throws IOException, InterruptedException {
        String[] rowSplits = row.toString().split(",");
        String[] catSplits = rowSplits[rowSplits.length - 1].toLowerCase().split("\\|");
        for (String cat : catSplits) {
            context.write(new Text(cat), one);
        }
    }
}
