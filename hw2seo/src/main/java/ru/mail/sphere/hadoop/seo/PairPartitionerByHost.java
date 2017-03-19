package ru.mail.sphere.hadoop.seo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PairPartitionerByHost extends Partitioner<HostQueryPair, IntWritable> {
    @Override
    public int getPartition(HostQueryPair hostQueryPair, IntWritable intWritable, int numPartitions) {
        return Math.abs(hostQueryPair.getHost().hashCode()) % numPartitions;
    }
}
