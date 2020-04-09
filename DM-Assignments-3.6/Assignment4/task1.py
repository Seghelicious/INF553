from graphframes import GraphFrame
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
import time
import os
import sys


os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 pyspark-shell"


def write_to_file(output_file, output):
    f = open(output_file, 'w')
    for row in output:
        line = ""
        for node in row:
            line = line + "'" + str(node) + "', "
        line = line[:-2]
        f.write(line)
        f.write('\n')
    f.close()


# time /home/local/spark/latest/bin/spark-submit --packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 task1.py $ASNLIB/publicdata/power_input.txt task1.csv
start_time = time.time()
input_file = sys.argv[1]
output_file = sys.argv[2]
# input_file = 'dataset/power_input.txt'
# output_file = 'output/task1.csv'

conf = SparkConf().setAppName("INF553").setMaster('local[*]')
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)

input_data = sc.textFile(input_file)
input_data = input_data.map(lambda x: x.split(" "))

vertices_src = input_data.map(lambda x: x[0]).persist()
vertices_dst = input_data.map(lambda x: x[1]).persist()
vertices = sc.union([vertices_src, vertices_dst]).distinct().map(lambda x: Row(x))

edges_forward = input_data.map(lambda x: (x[0], x[1])).persist()
edges_backward = input_data.map(lambda x: (x[1], x[0])).persist()
edges = sc.union([edges_forward, edges_backward]).distinct()

vertices = sqlContext.createDataFrame(vertices, ["id"])
edges = sqlContext.createDataFrame(edges, ["src", "dst"])

gf = GraphFrame(vertices, edges)
lpa_df = gf.labelPropagation(maxIter=5)
output = lpa_df.rdd.map(lambda x: (x[1], x[0])).groupByKey().mapValues(lambda x: sorted(list(x))) \
    .sortBy(lambda x: (len(x[1]), x[1])).map(lambda x: tuple(x[1])).collect()
write_to_file(output_file, output)
print("Duration:", time.time() - start_time)
