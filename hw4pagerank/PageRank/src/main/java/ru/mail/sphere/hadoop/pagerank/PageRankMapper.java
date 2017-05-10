package ru.mail.sphere.hadoop.pagerank;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class PageRankMapper extends Mapper<LongWritable, Text, IntWritable, NodeWritable> {
    private static List<Integer> emptyList = new LinkedList<>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] args = value.toString().trim().split("\\s+");

        int   node = Integer.valueOf(args[0]);
        float rank = Float.valueOf(args[1]);
        List<Integer> adjacencyList = new LinkedList<>();

        for (int i = 2; i < args.length; i++) {
            adjacencyList.add(Integer.valueOf(args[i]));
        }

        context.write(new IntWritable(node), new NodeWritable(-1.0f, adjacencyList));

        if (adjacencyList.size() > 0) {
            rank = rank / adjacencyList.size();

            for (Integer nodeIndex : adjacencyList) {
                context.write(new IntWritable(nodeIndex), new NodeWritable(rank, emptyList));
            }
        }
    }
}
