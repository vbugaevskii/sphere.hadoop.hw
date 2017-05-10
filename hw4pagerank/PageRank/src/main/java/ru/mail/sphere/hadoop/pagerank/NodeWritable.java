package ru.mail.sphere.hadoop.pagerank;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.LinkedList;
import java.util.List;

public class NodeWritable implements Writable {
    private float probability;
    private List<Integer> adjacencyList;

    public NodeWritable() {
        probability = 0.0f;
        adjacencyList = new LinkedList<>();
    }

    public NodeWritable(float probability, List<Integer> adjacencyList) {
        this.probability = probability;
        this.adjacencyList = new LinkedList<>(adjacencyList);
    }

    public void write(DataOutput out) throws IOException {
        out.writeFloat(probability);
        out.writeInt(adjacencyList.size());
        for (Integer nodeIndex: adjacencyList) {
            out.writeInt(nodeIndex);
        }
    }

    public void readFields(DataInput in) throws IOException {
        probability = in.readFloat();
        adjacencyList = new LinkedList<>();

        int adjacencyListSize = in.readInt();
        for (; adjacencyListSize > 0; adjacencyListSize--) {
            adjacencyList.add(in.readInt());
        }
    }

    public float getProbability() {
        return probability;
    }

    public List<Integer> getAdjacencyList() {
        return adjacencyList;
    }

    public int getAdjacencyListSize() {
        return adjacencyList.size();
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(probability);
        stringBuilder.append(" ");
        for (Integer index : adjacencyList) {
            stringBuilder.append(index);
            stringBuilder.append(" ");
        }

        return stringBuilder.toString();
    }
}
