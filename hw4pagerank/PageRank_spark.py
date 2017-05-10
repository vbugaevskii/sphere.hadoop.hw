import sys
from operator import add, itemgetter

from pyspark import SparkContext, SparkConf

N = 4847571
alpha = 0.85


def compute_emit_rank(x):
    node, (nodes_out, rank) = x
    
    emit = [(node, 0)]
    if nodes_out is not None and len(nodes_out) > 0:
        rank_ = float(rank) / len(nodes_out)
        emit += [(node, rank_) for node in nodes_out]
    return emit


def compute_total_rank(rank):
    if rank is None:
        rank = 0
    return alpha * rank + (1 - alpha) / N


def main(n_iter):
    conf = SparkConf().setAppName("PageRankApplication")
    sc = SparkContext(conf=conf)
    
    lines = sc.textFile(sys.argv[1])
    edges = lines.filter(lambda x: len(x) > 0 and x[0] != '#') \
                 .map(lambda x: map(int, x.strip().split()))   \
                 .groupByKey().cache()
                 
    prob = 1.0 / N
    ranks = edges.map(lambda (node, nodes_out): (node, prob))

    for i in range(n_iters):
        emits = edges.fullOuterJoin(ranks).flatMap(compute_emit_rank)
        ranks = emits.reduceByKey(add).mapValues(compute_total_rank)

    ranks = ranks.sortBy(itemgetter(1), ascending=False)

    for node, rank in ranks.take(10):
        print node, rank


if __name__ == "__main__":    
    if len(sys.argv) != 3:
        print "USE ARGUMENTS: <file_name> <n_iterations>"
        exit(1)
    else:
        n_iters = int(sys.argv[2])
        print "{} iters".format(n_iters)
        main(n_iters)
