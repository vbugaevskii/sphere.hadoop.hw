package ru.mail.sphere.hadoop.seo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

public class SEOMapper extends Mapper<LongWritable, Text, HostQueryPair, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String record = value.toString();
        String[] parts = record.split("\t");

        if (parts.length == 2) {
            try {
                URL url = new URL(parts[1]);
                context.write(new HostQueryPair(url.getHost(), parts[0]), one);
            } catch (MalformedURLException err) {
                context.getCounter("COMMON_COUNTERS", "SpoiledUrls").increment(1);
            }
        } else {
            context.getCounter("COMMON_COUNTERS", "SpoiledRecords").increment(1);
        }
    }
}
