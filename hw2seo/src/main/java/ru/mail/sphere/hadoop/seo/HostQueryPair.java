package ru.mail.sphere.hadoop.seo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.Objects;

public class HostQueryPair implements WritableComparable<HostQueryPair> {
    private Text host, query;

    public HostQueryPair() {
        this.host = new Text();
        this.query = new Text();
    }

    public HostQueryPair(String host, String query) {
        this.host = new Text(host);
        this.query = new Text(query);
    }

    public HostQueryPair(Text host, Text query) {
        this.host = host;
        this.query = query;
    }

    public Text getHost() {
        return host;
    }

    public Text getQuery() {
        return query;
    }

    public int compareTo(@Nonnull HostQueryPair that) {
        int compareResult = this.host.compareTo(that.host);
        return (compareResult == 0) ? this.query.compareTo(that.query) : compareResult;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || !(o instanceof HostQueryPair)) {
            return false;
        }

        HostQueryPair that = (HostQueryPair) o;
        return Objects.equals(this.host, that.host) && Objects.equals(this.query, that.query);
    }

    @Override
    public int hashCode() {
        int result = (host != null) ? host.hashCode() : 0;
        result = 31 * result + ((query != null) ? query.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return host + "\t" + query;
    }

    public void write(DataOutput out) throws IOException {
        host.write(out);
        query.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        host.readFields(in);
        query.readFields(in);
    }
}
