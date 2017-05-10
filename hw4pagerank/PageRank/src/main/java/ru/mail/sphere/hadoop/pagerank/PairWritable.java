package ru.mail.sphere.hadoop.pagerank;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PairWritable implements WritableComparable<PairWritable> {
    private int   node;
    private float rank;

    PairWritable() {
        node = 0;
        rank = 0.0f;
    }

    PairWritable(int node, float rank) {
        this.node = node;
        this.rank = rank;
    }

    public int getNode() {
        return node;
    }

    public float getRank() {
        return rank;
    }

    @Override
    public int compareTo(PairWritable that) {
        int compareResult = Float.compare(that.rank, this.rank);
        return (compareResult == 0) ? Integer.compare(this.node, that.node) : compareResult;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || !(o instanceof PairWritable)) {
            return false;
        }

        PairWritable that = (PairWritable) o;
        return Objects.equals(this.node, that.node) && Objects.equals(this.rank, that.rank);
    }

    @Override
    public int hashCode() {
        Integer node = this.node;
        Float   rank = this.rank;
        int result = node.hashCode();
        result = 31 * result + rank.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return node + "\t" + rank;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(node);
        out.writeFloat(rank);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        node = in.readInt();
        rank = in.readFloat();
    }
}
