package ru.mail.sphere.hadoop.hbase;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class HostSortComparator extends WritableComparator {
    HostSortComparator() {
        super(HostWritable.class, true);
    }

    @Override
    public int compare(WritableComparable value1, WritableComparable value2) {
        return ((HostWritable) value1).compareTo((HostWritable) value2);
    }
}
