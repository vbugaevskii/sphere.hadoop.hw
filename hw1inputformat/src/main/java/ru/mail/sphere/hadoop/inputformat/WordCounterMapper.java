package ru.mail.sphere.hadoop.inputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCounterMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private final static LongWritable one = new LongWritable(1);
    private final static Pattern wordRegPattern = Pattern.compile("\\p{L}+");
    private HashSet<String> words = new HashSet<String>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        words.clear();

        Matcher matcher = wordRegPattern.matcher(value.toString());

        while (matcher.find()) {
            String word = matcher.group().toLowerCase();
            if (word.length() > 0) {
                words.add(word);
            }
        }

        for (String word : words) {
            context.write(new Text(word), one);
        }
    }
}
