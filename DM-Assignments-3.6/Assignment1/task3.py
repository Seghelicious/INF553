import sys
import json
import time
from pyspark import SparkContext, SparkConf


def write_output_a(output_file_path, data):
    with open(output_file_path, 'w') as o1:
        o1.write("city,stars\n")
        for i in data:
            o1.write(i[0] + "," + str(i[1]) + "\n")


def write_output_b(output_file_path, data):
    with open(output_file_path, "w") as output_file:
        json.dump(data, output_file)


def task3(review_json, business_json, output_a, output_b):
    conf = SparkConf().setAppName("INF553").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    reviews_rdd = sc.textFile(review_json).map(lambda x: json.loads(x)).repartition(8).map(lambda x: (x["business_id"], x["stars"]))
    business_rdd = sc.textFile(business_json).map(lambda x: json.loads(x)).repartition(8).map(lambda x: (x["business_id"], x["city"]))
    city_rating = business_rdd.join(reviews_rdd).map(lambda x: (x[1][0], x[1][1])).groupByKey().map(
        lambda x: (x[0], sum(x[1]) / len(x[1])))
    m1_start_time = time.time()
    output_data_a = city_rating.collect()
    output_data_a.sort(key= lambda x: (-x[1], x[0]))
    count = 0
    for rating in output_data_a:
        if count == 10:
            break
        count += 1
        print(rating)
    m1 = time.time() - m1_start_time

    m2_start_time = time.time()
    print(city_rating.takeOrdered(10, key=lambda x: (-x[1], x[0])))
    m2 = time.time() - m2_start_time

    output_data_b = {"m1": m1, "m2": m2}
    write_output_a(output_a, output_data_a)
    write_output_b(output_b, output_data_b)
    sc.stop()


review_filepath = sys.argv[1]
business_filepath = sys.argv[2]
output_filepath_question_a = sys.argv[3]
output_filepath_question_b = sys.argv[4]
task3(review_filepath, business_filepath, output_filepath_question_a, output_filepath_question_b)
