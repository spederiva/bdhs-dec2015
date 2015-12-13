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

public class WCRunner extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        if(args.length < 1) {
            System.out.println("*****************************************************************");
            System.out.println("* USAGE: hadoop jar <JAR_PATH>.jar <INPUT_PATH> [<OUTPUT_PATH>] *");
            System.out.println("* NOTE: If no output path is specified,                         *");
            System.out.println("*       Input will be INPUT_PATH/input                          *");
            System.out.println("*       and OUTPUT_PTH will be INPUT_PTH/output                 *");
            System.out.println("*****************************************************************");
            System.exit(1);
        }
        WCRunner runner = new WCRunner();
        System.exit(ToolRunner.run(runner, args));
    }

    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration, "Basic Word Count");
        Path inputPath, outputPath;

        if(args.length == 1) {
            inputPath = new Path(args[0] + "/input");
            outputPath = new Path(args[0] + "/output");
        } else if(args.length > 1) {
            inputPath = new Path(args[0]);
            outputPath = new Path(args[1]);
        } else {
            throw new IllegalArgumentException("no argument passed as the INPUT PATH");
        }

        // for development - delete previous run results to prevent fail.
        FileSystem fs = FileSystem.get(configuration);
        if(fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        job.setJarByClass(WCRunner.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, inputPath);
        TextOutputFormat.setOutputPath(job, outputPath);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
