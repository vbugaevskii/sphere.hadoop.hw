package ru.mail.sphere.hadoop.seo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SEOCombiner extends Reducer<HostQueryPair, IntWritable, HostQueryPair, IntWritable> {
    @Override
    protected void reduce(HostQueryPair key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        Text hostCurrent = new Text(key.getHost());

        Text queryCurrent = new Text(key.getQuery());
        int sumCurrent = 0;

        for (IntWritable counts: values) {
            Text query = key.getQuery();

            if (!query.equals(queryCurrent)) {
                if (sumCurrent > 0) {
                    context.write(new HostQueryPair(hostCurrent, queryCurrent), new IntWritable(sumCurrent));
                }

                sumCurrent = 0;
                queryCurrent.set(query);
            }

            sumCurrent += counts.get();
        }

        if (sumCurrent > 0) {
            context.write(new HostQueryPair(hostCurrent, queryCurrent), new IntWritable(sumCurrent));
        }
    }
}
