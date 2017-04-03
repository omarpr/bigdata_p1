package edu.uprm.cse.bigdata.ex6;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by omar on 3/30/17.
 */
public class TweetFinderCounterByScreenNameDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TweetFinderCounterByScreenNameDriver <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(TweetFinderCounterByScreenNameDriver.class);
        job.setJobName("TweetFinderCounterByScreenName");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(TweetFinderCounterByScreenNameMapper.class);
        job.setReducerClass(TweetFinderCounterByScreenNameReducer.class);
        //job.setCombinerClass(class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //System.println("Done!");

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
