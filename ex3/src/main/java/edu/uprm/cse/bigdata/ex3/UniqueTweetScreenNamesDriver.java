package edu.uprm.cse.bigdata.ex3;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by omar on 3/30/17.
 */
public class UniqueTweetScreenNamesDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: UniqueTweetScreenNamesDriver <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(UniqueTweetScreenNamesDriver.class);
        job.setJobName("UniqueTweetScreenNames");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(UniqueTweetScreenNamesMapper.class);
        job.setReducerClass(UniqueTweetScreenNamesReducer.class);
        //job.setCombinerClass(class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //System.println("Done!");

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
