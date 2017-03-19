package ru.mail.sphere.hadoop.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BlocksInputFormat extends FileInputFormat<LongWritable, Text> {
    private static final String BYTES_PER_MAP = "mapreduce.input.indexedgz.bytespermap";

    public List<InputSplit> getSplits(JobContext context) throws IOException {
        List<InputSplit> splits = new ArrayList<InputSplit>();

        for (FileStatus fileStatus : listStatus(context)) {
            splits.addAll(getSplitsPerFile(fileStatus, context.getConfiguration()));
        }

        return splits;
    }

    private List<InputSplit> getSplitsPerFile(FileStatus fileStatus, Configuration config) throws IOException {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        long bytesPerSplit = getNumBytesPerSplit(config);

        Path pagesPath = fileStatus.getPath();
        Path indexPath = pagesPath.suffix(".idx");

        FileSystem fileSystem = pagesPath.getFileSystem(config);

        FSDataInputStream indexInputStream = fileSystem.open(indexPath);

        long offsetPagesSplit = 0;
        long lengthPagesSplit = 0;

        List<Integer> lengthPagesSplitList = new ArrayList<Integer>();

        while (true) {
            try {
                int sizeOfPage = Integer.reverseBytes(indexInputStream.readInt());

                lengthPagesSplit += (long) sizeOfPage;
                lengthPagesSplitList.add(sizeOfPage);

                if (lengthPagesSplit >= bytesPerSplit) {
                    splits.add(new BlocksFileSplit(pagesPath, offsetPagesSplit, lengthPagesSplit, lengthPagesSplitList));

                    offsetPagesSplit += lengthPagesSplit;
                    lengthPagesSplit = 0;
                    lengthPagesSplitList.clear();
                }
            } catch (EOFException err) {
                break;
            }
        }

        if (!lengthPagesSplitList.isEmpty()) {
            splits.add(new BlocksFileSplit(pagesPath, offsetPagesSplit, lengthPagesSplit, lengthPagesSplitList));
        }

        indexInputStream.close();
        return splits;
    }

    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        BlocksRecordReader recordReader = new BlocksRecordReader();
        recordReader.initialize(split, context);
        return recordReader;
    }

    private static long getNumBytesPerSplit(Configuration conf) {
        return conf.getLong(BYTES_PER_MAP, 32L * 1024L * 1024L);
    }
}
