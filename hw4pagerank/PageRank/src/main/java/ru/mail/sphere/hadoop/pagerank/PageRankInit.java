package ru.mail.sphere.hadoop.pagerank;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class PageRankInit extends Configured implements Tool {
    static class InitMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.length() > 0 && !line.startsWith("#")) {
                String[] pair = line.split("\t");
                context.write(
                        new IntWritable(Integer.valueOf(pair[0])),
                        new IntWritable(Integer.valueOf(pair[1]))
                );
            }
        }
    }

    static class InitReducer extends Reducer<IntWritable, IntWritable, IntWritable, NodeWritable> {
        private float rank = 1.0f / PageRankJob.N;

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            List<Integer> adjacencyList = new LinkedList<>();
            for (IntWritable value : values) {
                adjacencyList.add(value.get());
            }
            context.write(key, new NodeWritable(rank, adjacencyList));
        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());

        job.setJarByClass(PageRankResults.class);
        job.setJobName("PageRank. Init");

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(InitMapper.class);
        job.setReducerClass(InitReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NodeWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(10);
        return job;
    }

    public int run(String[] args) throws Exception {
        Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new PageRankInit(), args);
        System.exit(ret);
    }
}
