#!/bin/bash 

hadoop fs -rm -r pagerank_out
hadoop fs -mkdir pagerank_out

TIME_STARTED=$(date +%s)
ITERS=10

hadoop jar pagerank-1.0.jar \
    ru.mail.sphere.hadoop.pagerank.PageRankInit \
    /data/hw5/soc-LiveJournal1.txt pagerank_out/it00
    
hadoop jar pagerank-1.0.jar \
    ru.mail.sphere.hadoop.pagerank.PageRankJob \
    pagerank_out $ITERS
    
INPUT_PATH=$(printf "pagerank_out/it%02d/part-r-*" $ITERS) 

hadoop jar pagerank-1.0.jar \
    ru.mail.sphere.hadoop.pagerank.PageRankResults \
    pagerank_out/it$ITERS/part-r-* pagerank_out/result

TIME_STOPPED=$(date +%s)
TIME_PROCESSED=$(($TIME_STOPPED - $TIME_STARTED))
echo "Elapsed: $(($TIME_PROCESSED / 60))mins, $(($TIME_PROCESSED % 60))sec"
    
hadoop fs -cat pagerank_out/result/part-r-* | head -n 10
