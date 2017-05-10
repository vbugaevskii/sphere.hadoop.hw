package ru.mail.sphere.hadoop.pagerank;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class PageRankResults extends Configured implements Tool {
    static class ResultsMapper extends Mapper<LongWritable, Text, PairWritable, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] args = value.toString().trim().split("\\s+");

            int   node = Integer.valueOf(args[0]);
            float rank = Float.valueOf(args[1]);

            context.write(new PairWritable(node, rank), NullWritable.get());
        }
    }

    static class ResultsReducer extends Reducer<PairWritable, NullWritable, IntWritable, FloatWritable> {
        @Override
        protected void reduce(PairWritable key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(
                    new IntWritable(key.getNode()),
                    new FloatWritable(key.getRank())
            );
        }
    }

    static class ResultsSortComparator extends WritableComparator {
        ResultsSortComparator() {
            super(PairWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((PairWritable) a).compareTo((PairWritable) b);
        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());

        job.setJarByClass(PageRankResults.class);
        job.setJobName("PageRank. Result");

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(ResultsMapper.class);
        job.setReducerClass(ResultsReducer.class);

        job.setMapOutputKeyClass(PairWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setSortComparatorClass(ResultsSortComparator.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(1);
        return job;
    }

    public int run(String[] args) throws Exception {
        Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new PageRankResults(), args);
        System.exit(ret);
    }
}

