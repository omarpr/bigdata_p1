package edu.uprm.cse.bigdata.ex1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by omar on 3/30/17.
 */
public class SelectedWordsInTweetCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();
        List<String> toFind = Arrays.asList("TRUMP", "MAGA", "DICTATOR", "IMPEACH", "DRAIN", "SWAMP", "CHANGE");
        List<String> evaluatedWord = new ArrayList<String>();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            String tweet = status.getText().toUpperCase();
            String[] words;

            tweet = tweet.replace("\r", " ").replace("\n", " ").replace("\t", " ")
                    .replace("\"", "").replace("'", "").replace(".", "").replace(",", "").trim();
            words = tweet.split(" ");


            for (String word : words) {
                if (!evaluatedWord.contains(word) && toFind.contains(word)) {
                    evaluatedWord.add(word);
                    context.write(new Text(word), new IntWritable(1));
                }
            }
        } catch (TwitterException e) {

        }
    }
}
