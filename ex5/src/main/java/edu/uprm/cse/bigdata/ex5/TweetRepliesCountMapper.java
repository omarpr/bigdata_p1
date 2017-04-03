package edu.uprm.cse.bigdata.ex5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.*;

import java.io.IOException;

/**
 * Created by omar on 3/30/17.
 */
public class TweetRepliesCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            long replyStatusID = status.getInReplyToStatusId();

            if (replyStatusID > 0) {
                Twitter twitter = new TwitterFactory(LocalConfigurationBuilder.getConfigurationBuilded()).getInstance();
                Status repliedStatus = twitter.showStatus(replyStatusID);
                String tweet = repliedStatus.getText().replace("\r", " ").replace("\n", " ").replace("\t", " ").trim();

                context.write(new Text(tweet), new IntWritable(1));
            }
        } catch (TwitterException e) {

        }
    }
}
