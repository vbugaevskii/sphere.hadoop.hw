#!/bin/bash 

spark-submit --master yarn --deploy-mode cluster \
    --num-executors 10 --executor-cores 2 --executor-memory 3G \
    PageRank_spark.py /data/hw5/soc-LiveJournal1.txt 10
