package ru.mail.sphere.hadoop.inputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCounterReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long totalNumber = 0;
        for (LongWritable count : values) {
            totalNumber += count.get();
        }
        context.write(key, new LongWritable(totalNumber));
    }
}
