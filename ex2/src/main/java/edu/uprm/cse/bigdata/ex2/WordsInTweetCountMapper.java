package edu.uprm.cse.bigdata.ex2;

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
public class WordsInTweetCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();
        List<String> stopWords = Arrays.asList("a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
                "has", "he", "in", "is", "it", "its", "of", "on", "that", "the", "to", "was", "were", "will", "with");

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            String tweet = status.getText().toUpperCase();
            String words[];
            List<String> evaluatedWord = new ArrayList<String>();

            tweet = tweet.replace("\r", " ").replace("\n", " ").replace("\t", " ")
                    .replace("\"", "").replace("'", "").replace(".", "").replace(",", "").trim().toUpperCase();
            words = tweet.split(" ");

            for (String word : words) {
                if (!evaluatedWord.contains(word) && !stopWords.contains(word.toLowerCase())) {
                    evaluatedWord.add(word);
                    context.write(new Text(word), new IntWritable(1));
                }
            }
        } catch (TwitterException e) {

        }
    }
}
