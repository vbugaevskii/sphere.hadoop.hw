package ru.mail.sphere.hadoop.hbase;

import javax.annotation.Nonnull;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.Objects;

public class HostWritable implements WritableComparable<HostWritable> {
    private Text host, table;

    HostWritable() {
        host = new Text();
        table = new Text();
    }

    HostWritable(String host, String fromTable) {
        this.host = new Text(host);
        this.table = new Text(fromTable);
    }

    public Text getHost() {
        return host;
    }

    public Text getTable() {
        return table;
    }

    public int compareTo(@Nonnull HostWritable that) {
        int compareResult = this.host.compareTo(that.host);
        return (compareResult != 0) ? compareResult : this.table.compareTo(that.table);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || !(o instanceof HostWritable)) {
            return false;
        }

        HostWritable that = (HostWritable) o;
        return Objects.equals(this.host, that.host) && Objects.equals(this.table, that.table);
    }

    public void write(DataOutput out) throws IOException {
        host.write(out);
        table.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        host.readFields(in);
        table.readFields(in);
    }
}
