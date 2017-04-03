package edu.uprm.cse.bigdata.ex5;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by omar on 3/30/17.
 */
public class TweetRepliesCountDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TweetRepliesCountDriver <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(TweetRepliesCountDriver.class);
        job.setJobName("TweetRepliesCount");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(TweetRepliesCountMapper.class);
        job.setReducerClass(TweetRepliesCountReducer.class);
        //job.setCombinerClass(class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //System.println("Done!");

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
