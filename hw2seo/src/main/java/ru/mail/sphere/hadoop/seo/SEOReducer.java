package ru.mail.sphere.hadoop.seo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SEOReducer extends Reducer<HostQueryPair, IntWritable, HostQueryPair, IntWritable> {
    private int numberClicksMin = 1;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration config = context.getConfiguration();
        numberClicksMin = config.getInt("seo.minclicks", numberClicksMin);
    }

    @Override
    protected void reduce(HostQueryPair key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        Text hostCurrent = new Text(key.getHost());

        Text queryCurrent = new Text(key.getQuery());
        int sumCurrent = 0;

        Text queryBest = new Text(key.getQuery());
        int sumBest = -1;

        for (IntWritable counts: values) {
            Text query = key.getQuery();

            if (!query.equals(queryCurrent)) {
                if (sumCurrent > sumBest || (sumCurrent == sumBest && queryCurrent.compareTo(queryBest) < 0)) {
                    sumBest = sumCurrent;
                    queryBest.set(queryCurrent);
                }

                sumCurrent = 0;
                queryCurrent.set(query);
            }

            sumCurrent += counts.get();
        }

        if (sumCurrent > sumBest || (sumCurrent == sumBest && queryCurrent.compareTo(queryBest) < 0)) {
            sumBest = sumCurrent;
            queryBest.set(queryCurrent);
        }

        if (sumBest >= numberClicksMin) {
            context.write(new HostQueryPair(hostCurrent, queryBest), new IntWritable(sumBest));
        }
    }
}
