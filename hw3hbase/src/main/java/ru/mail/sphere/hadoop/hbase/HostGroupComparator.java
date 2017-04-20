package ru.mail.sphere.hadoop.hbase;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class HostGroupComparator extends WritableComparator {
    HostGroupComparator() {
        super(HostWritable.class, true);
    }

    public int compare(WritableComparable value1, WritableComparable value2) {
        Text host1 = ((HostWritable) value1).getHost();
        Text host2 = ((HostWritable) value2).getHost();
        return host1.compareTo(host2);
    }
}
