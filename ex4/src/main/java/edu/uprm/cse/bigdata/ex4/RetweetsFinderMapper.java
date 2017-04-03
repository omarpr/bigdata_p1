package edu.uprm.cse.bigdata.ex4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;

/**
 * Created by omar on 3/30/17.
 */
public class RetweetsFinderMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            String tweet;

            if (status.isRetweet()) {
                tweet = status.getRetweetedStatus().getText().replace("\t", " ").replace("\r", " ").replace("\n", " ").trim();
                context.write(new Text(tweet), new IntWritable(1));
            }
        } catch (TwitterException e) {

        }
    }
}
