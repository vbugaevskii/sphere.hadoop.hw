package ru.mail.sphere.hadoop.hbase;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class HostPartitioner extends Partitioner<HostWritable, Text> {
    @Override
    public int getPartition(HostWritable hostQueryPair, Text intWritable, int numPartitions) {
        return Math.abs(hostQueryPair.getHost().hashCode()) % numPartitions;
    }
}
