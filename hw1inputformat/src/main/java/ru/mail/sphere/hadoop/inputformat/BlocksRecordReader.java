package ru.mail.sphere.hadoop.inputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class BlocksRecordReader extends RecordReader<LongWritable, Text> {
    private static final int MAX_BUFFER_SIZE = 2048;
    private static byte[] bytesOfPageDecompressed = new byte[MAX_BUFFER_SIZE];

    private FSDataInputStream pagesInputStream;
    private List<Integer> pagesLengths;

    private int pagesLengthCurrentPos;

    private LongWritable key;
    private Text value;

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        BlocksFileSplit blocksSplit = (BlocksFileSplit) split;

        Path pagesPath = blocksSplit.getPath();
        FileSystem fileSystem = pagesPath.getFileSystem(context.getConfiguration());

        pagesInputStream = fileSystem.open(pagesPath);
        pagesInputStream.seek(blocksSplit.getStart());

        pagesLengths = blocksSplit.getPagesLengthList();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (pagesLengthCurrentPos >= pagesLengths.size()) {
            key = null;
            value = null;

            return false;
        } else {
            if (key == null) {
                key = new LongWritable(pagesInputStream.getPos());
            } else {
                key.set(pagesInputStream.getPos());
            }

            if (value == null) {
                value = new Text();
            } else {
                value.clear();
            }

            int sizeOfPageCompressed = pagesLengths.get(pagesLengthCurrentPos++);

            byte[] bytesOfPageCompressed = new byte[sizeOfPageCompressed];
            pagesInputStream.readFully(bytesOfPageCompressed, 0, sizeOfPageCompressed);

            Inflater decompressor = new Inflater();
            decompressor.setInput(bytesOfPageCompressed);

            while (true) {
                try {
                    int numberBytesRead = decompressor.inflate(bytesOfPageDecompressed);

                    if (numberBytesRead > 0) {
                        value.append(bytesOfPageDecompressed, 0, numberBytesRead);
                    } else {
                        break;
                    }
                } catch (DataFormatException err) {
                    throw new IOException("Failed to decompress!");
                }
            }

            decompressor.end();
            return true;
        }
    }

    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return pagesLengths.isEmpty() ? 1.0f : (float) (pagesLengthCurrentPos) / pagesLengths.size();
    }

    public void close() throws IOException {
        if (pagesInputStream != null) {
            pagesInputStream.close();
            pagesInputStream = null;
        }
    }
}
