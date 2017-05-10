import sys
from operator import add, itemgetter

from pyspark import SparkContext, SparkConf

N = 4847571
alpha = 0.85


def compute_emit_rank(x):
    node, (nodes_out, rank) = x
    if nodes_out is not None and len(nodes_out) > 0:
        rank_ = float(rank) / len(nodes_out)
        return [(node, rank_) for node in nodes_out]
    else:
        return []


def compute_total_rank(x):
    node, (nodes_out, rank) = x
    
    if rank is None:
        rank = 0
    
    rank = alpha * rank + (1 - alpha) / N
    return node, (nodes_out, rank)


def main(n_iter):
    conf = SparkConf().setAppName("PageRankApplication")
    sc = SparkContext(conf=conf)
    
    lines = sc.textFile(sys.argv[1])
    edges = lines.filter(lambda x: len(x) > 0 and x[0] != '#') \
                 .map(lambda x: map(int, x.strip().split()))   \
                 .groupByKey().cache()
    
    prob = 1.0 / N
    graph = edges.mapValues(lambda x: (x, prob))

    for i in range(n_iters):
        emits = graph.flatMap(compute_emit_rank).reduceByKey(add)
        graph = edges.fullOuterJoin(emits).map(compute_total_rank)

    ranks = graph.mapValues(itemgetter(1)) \
                 .sortBy(itemgetter(1), ascending=False)

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
