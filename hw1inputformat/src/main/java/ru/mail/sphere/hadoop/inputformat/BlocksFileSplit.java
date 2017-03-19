package ru.mail.sphere.hadoop.inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BlocksFileSplit extends FileSplit {
    private List<Integer> pagesLengthList;

    public BlocksFileSplit() {
        super();
    }

    public BlocksFileSplit(Path path, long start, long length, List<Integer> pagesLengthList) {
        super(path, start, length, new String[]{});
        this.pagesLengthList = new ArrayList<Integer>(pagesLengthList);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        out.writeInt(pagesLengthList.size());
        for (Integer pageLength : pagesLengthList) {
            out.writeInt(pageLength);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        pagesLengthList = new ArrayList<Integer>();

        int numberOfPages = in.readInt();
        for (; numberOfPages > 0; numberOfPages--) {
            pagesLengthList.add(in.readInt());
        }
    }

    public List<Integer> getPagesLengthList() {
        return pagesLengthList;
    }
}
