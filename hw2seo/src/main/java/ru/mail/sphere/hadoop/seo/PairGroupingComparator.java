package ru.mail.sphere.hadoop.seo;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PairGroupingComparator extends WritableComparator {
    PairGroupingComparator() {
        super(HostQueryPair.class, true);
    }

    public int compare(WritableComparable value1, WritableComparable value2) {
        Text host1 = ((HostQueryPair) value1).getHost();
        Text host2 = ((HostQueryPair) value2).getHost();
        return host1.compareTo(host2);
    }
}
