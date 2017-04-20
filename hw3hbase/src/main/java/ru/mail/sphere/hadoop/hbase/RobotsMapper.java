package ru.mail.sphere.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;

public class RobotsMapper extends TableMapper<HostWritable, Text> {
    private byte[] pagesTableName, sitesTableName;

    private byte[] cfDocs = Bytes.toBytes("docs");
    private byte[] columnUrl = Bytes.toBytes("url");
    private byte[] columnDisabled = Bytes.toBytes("disabled");

    private byte[] cfInfo = Bytes.toBytes("info");
    private byte[] columnSite = Bytes.toBytes("site");
    private byte[] columnRobots = Bytes.toBytes("robots");

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        Configuration config = context.getConfiguration();
        pagesTableName = Bytes.toBytes(config.get(RobotsJob.PAGES_TABLE_NAME));
        sitesTableName = Bytes.toBytes(config.get(RobotsJob.SITES_TABLE_NAME));
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result columns, Context context)
            throws IOException, InterruptedException {
        TableSplit splitCurrent = (TableSplit) context.getInputSplit();
        byte[] currentTableName = splitCurrent.getTableName();

        if (Arrays.equals(currentTableName, pagesTableName)) {
            String urlName = new String(columns.getValue(cfDocs, columnUrl));
            String urlDisabled = ((columns.getValue(cfDocs, columnDisabled)) == null) ? "N" : "Y";

            try {
                URL url = new URL(urlName);
                context.write(new HostWritable(url.getHost(), "1pages"), new Text(urlDisabled + urlName));
            } catch (MalformedURLException err) {
                context.getCounter("COMMON_COUNTERS", "SpoiledUrls").increment(1);
            }
        } else if (Arrays.equals(currentTableName, sitesTableName)) {
            byte[] bytesSite = columns.getValue(cfInfo, columnSite);
            byte[] bytesRobots = columns.getValue(cfInfo, columnRobots);

            if (bytesRobots != null && bytesSite != null) {
                String host = new String(bytesSite), robots = new String(bytesRobots);
                context.write(new HostWritable(host, "0sites"), new Text(robots));
            }
        }
    }
}
