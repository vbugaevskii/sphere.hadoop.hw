package ru.mail.sphere.hadoop.seo;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PairSortComparator extends WritableComparator {
    PairSortComparator() {
        super(HostQueryPair.class, true);
    }

    @Override
    public int compare(WritableComparable value1, WritableComparable value2) {
        return ((HostQueryPair) value1).compareTo((HostQueryPair) value2);
    }
}
