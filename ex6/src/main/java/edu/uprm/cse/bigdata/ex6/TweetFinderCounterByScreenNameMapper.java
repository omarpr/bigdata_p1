package edu.uprm.cse.bigdata.ex6;

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
public class TweetFinderCounterByScreenNameMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);

            Text k = new Text(status.getUser().getScreenName());
            LongWritable v = new LongWritable(status.getId());

            context.write(k, v);
        } catch (TwitterException e) {

        }
    }
}
