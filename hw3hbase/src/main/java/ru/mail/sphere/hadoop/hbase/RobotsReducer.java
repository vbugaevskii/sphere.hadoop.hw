package ru.mail.sphere.hadoop.hbase;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class RobotsReducer extends TableReducer<HostWritable, Text, ImmutableBytesWritable> {
    private static byte[] cf = Bytes.toBytes("docs");
    private static byte[] columnDisabled = Bytes.toBytes("disabled");

    @Override
    protected void reduce(HostWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        RobotsFilter filter = new RobotsFilter();

        for (Text value: values) {
            if (key.getTable().toString().equals("0sites")) {
                try {
                    filter.addRules(value.toString());
                } catch (RobotsFilter.BadFormatException err) {
                    context.getCounter("COMMON_COUNTERS", "SpoiledRobotsTxt").increment(1);
                }
            } else if (key.getTable().toString().equals("1pages")) {
                String url = value.toString().substring(1);

                boolean disabledPredicted = value.toString().substring(0, 1).equals("Y");
                boolean disabledActual = !filter.isAllowed(url);

                byte[] urlMD5Hash = Bytes.toBytes(MD5Hash.digest(url).toString());

                if (disabledActual && !disabledPredicted) {
                    Put put = new Put(urlMD5Hash).addColumn(cf, columnDisabled, Bytes.toBytes("Y"));
                    context.write(null, put);
                } else if (!disabledActual && disabledPredicted) {
                    Delete delete = new Delete(urlMD5Hash).addColumn(cf, columnDisabled);
                    context.write(null, delete);
                }
            }
        }
    }
}
