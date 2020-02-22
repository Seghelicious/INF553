import sys
import json
from pyspark import SparkContext, SparkConf


def write_output(output_file_path, data):
    with open(output_file_path, "w") as output_file:
        json.dump(data, output_file)


def task1(review_json, output):
    conf = SparkConf().setAppName("INF553").setMaster('local[*]')
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    rdd = sc.textFile(review_json).map(lambda x: json.loads(x))
    reviews_rdd = rdd.repartition(8).map(lambda x: (x["date"], x["user_id"], x["business_id"])).persist()

    # The total number of reviews
    n_review = reviews_rdd.count()

    # The number of reviews in 2018
    n_review_2018 = reviews_rdd.filter(lambda x: (x[0][0:4] == '2018')).count()

    # The number of distinct users who wrote reviews
    reviews_user = reviews_rdd.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x + y)
    n_user = reviews_user.count()

    # The top 10 users who wrote the largest numbers of reviews and the number of reviews they wrote
    top10_user = reviews_user.takeOrdered(10, key=lambda x: (-x[1], x[0]))

    # The number of distinct businesses that have been reviewed
    reviews_business = reviews_rdd.map(lambda x: (x[2], 1)).reduceByKey(lambda x, y: x + y)
    n_business = reviews_business.count()

    # The top 10 businesses that had the largest numbers of reviews and the number of reviews they had
    top10_business = reviews_business.takeOrdered(10, key=lambda x: (-x[1], x[0]))

    output_data = {'n_review': n_review,
                   'n_review_2018': n_review_2018,
                   'n_user': n_user,
                   'top10_user': top10_user,
                   'n_business': n_business,
                   'top10_business': top10_business}
    write_output(output, output_data)
    sc.stop()


review_filepath = sys.argv[1]
output_filepath = sys.argv[2]
task1(review_filepath, output_filepath)
