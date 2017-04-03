package edu.uprm.cse.bigdata.ex6;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by omar on 3/30/17.
 */
public class TweetFinderCounterByScreenNameReducer extends Reducer<Text, LongWritable, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        List<Long> ids = new ArrayList<Long>();
        String out;

        for (LongWritable value : values) {
            ids.add(value.get());
        }

        out = ids.size() + "\t" + StringUtils.join(ids, "\t");
        context.write(key, new Text(out));
    }
}