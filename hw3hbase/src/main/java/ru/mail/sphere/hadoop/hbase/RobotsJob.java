package ru.mail.sphere.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RobotsJob extends Configured implements Tool {
    public static final String PAGES_TABLE_NAME = "robots.pages_table_name";
    public static final String SITES_TABLE_NAME = "robots.sites_table_name";

    private Job getJobConf(String pagesTableName, String sitesTableName) throws IOException {
        Job job = Job.getInstance(getConf());

        job.setJarByClass(RobotsJob.class);
        job.setJobName("HW3: HBase");

        Scan scan1 = new Scan().addColumn(Bytes.toBytes("docs"), Bytes.toBytes("url")).
                                addColumn(Bytes.toBytes("docs"), Bytes.toBytes("disabled"));
        Scan scan2 = new Scan().addColumn(Bytes.toBytes("info"), Bytes.toBytes("site")).
                                addColumn(Bytes.toBytes("info"), Bytes.toBytes("robots"));

        scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(pagesTableName));
        scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(sitesTableName));

        List<Scan> scans = new ArrayList<>();
        scans.add(scan1);
        scans.add(scan2);

        Configuration config = job.getConfiguration();
        config.set(PAGES_TABLE_NAME, pagesTableName);
        config.set(SITES_TABLE_NAME, sitesTableName);

        TableMapReduceUtil.initTableMapperJob(
                scans,
                RobotsMapper.class,
                HostWritable.class,
                Text.class,
                job
        );

        TableMapReduceUtil.initTableReducerJob(
                pagesTableName,
                RobotsReducer.class,
                job
        );

        job.setPartitionerClass(HostPartitioner.class);
        job.setSortComparatorClass(HostSortComparator.class);
        job.setGroupingComparatorClass(HostGroupComparator.class);

        job.setNumReduceTasks(2);
        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new RobotsJob(), args);
        System.exit(ret);
    }
}
