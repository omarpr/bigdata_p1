package edu.uprm.cse.bigdata.ex5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by omar on 3/30/17.
 */
public class TweetRepliesCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int out = 0;

        for (IntWritable value : values) {
            out += value.get();
        }

        context.write(key, new IntWritable(out));
    }
}