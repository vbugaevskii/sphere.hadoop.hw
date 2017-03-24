package ru.mail.sphere.hadoop.seo;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SEOJob extends Configured implements Tool {
    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());

        job.setJarByClass(SEOJob.class);
        job.setJobName("HW2: SEO Optimization");

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(SEOMapper.class);
        job.setCombinerClass(SEOCombiner.class);
        job.setReducerClass(SEOReducer.class);

        job.setPartitionerClass(PairPartitionerByHost.class);
        job.setSortComparatorClass(PairSortComparator.class);
        job.setGroupingComparatorClass(PairGroupingComparator.class);

        job.setMapOutputKeyClass(HostQueryPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(HostQueryPair.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job;
    }


    public int run(String[] args) throws Exception {
        Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new SEOJob(), args);
        System.exit(ret);
    }
}
