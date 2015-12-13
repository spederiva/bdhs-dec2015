package com.sela.bdhs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CatCountRunner extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        CatCountRunner runner = new CatCountRunner();
        System.exit(ToolRunner.run(runner, args));
    }

    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();

        Job job = Job.getInstance(configuration, "Category Count");

        Path inputPath = new Path("/user/eyalb/ml-latest-small/movies.csv");
        Path outputPath = new Path("/user/eyalb/movies-cat-count");

        FileSystem fs = FileSystem.get(configuration);

        if(fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        job.setJarByClass(CatCountRunner.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, inputPath);
        TextOutputFormat.setOutputPath(job, outputPath);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(CatCountMapper.class);
        job.setReducerClass(CatCountReducer.class);
        job.setCombinerClass(CatCountReducer.class);

        return job.waitForCompletion(true) ? 0 : 1;

    }
}
