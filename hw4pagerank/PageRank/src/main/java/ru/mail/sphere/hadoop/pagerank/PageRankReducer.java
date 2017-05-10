package ru.mail.sphere.hadoop.pagerank;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PageRankReducer extends Reducer<IntWritable, NodeWritable, IntWritable, NodeWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<NodeWritable> values, Context context)
            throws IOException, InterruptedException {
        float rank = 0.0f;
        List<Integer> adjacencyList = new ArrayList<>();

        for (NodeWritable node : values) {
            if (node.getProbability() < 0) {
                adjacencyList.addAll(node.getAdjacencyList());
            } else {
                rank += node.getProbability();
            }
        }

        rank = PageRankJob.alpha * rank + (1.0f - PageRankJob.alpha) / PageRankJob.N;
        context.write(key, new NodeWritable(rank, adjacencyList));
    }
}
