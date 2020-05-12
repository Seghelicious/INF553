import sys
import json
import time
from pyspark import SparkContext, SparkConf


def write_output(output_file_path, data):
    with open(output_file_path, "w") as output_file:
        json.dump(data, output_file)


def task2(review_json, output, partitions):
    conf = SparkConf().setAppName("INF553").setMaster('local[*]')
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    rdd = sc.textFile(review_json).map(json.loads)

    reviews_rdd = rdd.map(lambda x: (x["business_id"], 1)).persist()
    default_num_partition = reviews_rdd.getNumPartitions()
    default_num_items = reviews_rdd.glom().map(len).collect()
    default_start = time.time()
    default_business_reviews = reviews_rdd.reduceByKey(lambda x, y: x + y).takeOrdered(10, key=lambda x: (-x[1], x[0]))
    default_time = time.time() - default_start

    custom_reviews_rdd = reviews_rdd.partitionBy(partitions, lambda x: hash(x) % partitions)
    custom_num_partitions = custom_reviews_rdd.getNumPartitions()
    custom_num_items = custom_reviews_rdd.glom().map(len).collect()
    custom_start = time.time()
    custom_business_review = custom_reviews_rdd.reduceByKey(lambda x, y: x + y).takeOrdered(10, key=lambda x: (-x[1], x[0]))
    custom_time = time.time() - custom_start

    output_data = {
        'default': {'n_partition': default_num_partition, 'n_items': default_num_items, 'exe_time': default_time},
        'customized': {'n_partition': custom_num_partitions, 'n_items': custom_num_items, 'exe_time': custom_time}}

    write_output(output, output_data)
    sc.stop()


review_filepath = sys.argv[1]
output_filepath = sys.argv[2]
n_partition = int(sys.argv[3])
task2(review_filepath, output_filepath, n_partition)
