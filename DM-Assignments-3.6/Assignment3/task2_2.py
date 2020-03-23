import math
import os
import sys
import json
import numpy as np
import xgboost as xgb
from pyspark import SparkContext, SparkConf
import time

# os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3.6"


def get_feature_list(row):
    features = []
    features.extend(user_json_map.get(row[0]))
    features.extend(business_json_map.get(row[1]))
    return features


def get_model_params():
    params = {}
    eta = 0.3
    booster = 'gbtree'
    max_depth = 15
    objective = 'reg:linear'
    silent = 1
    params['eta'] = eta
    params['booster'] = booster
    params['max-depth'] = max_depth
    params['objective'] = objective
    params['silent'] = silent
    return params


def write_to_file(output_file, output, test_data):
    file = open(output_file, 'w')
    file.write("user_id, business_id, prediction\n")
    for i in range(0, len(output)):
        predicted_rating = output[i]
        predicted_rating = max(1, min(5, predicted_rating))
        file.write(test_data[i][0] + "," + test_data[i][1] + "," + str(predicted_rating) + "\n")


start_time = time.time()
# time /home/local/spark/latest/bin/spark-submit task2_2.py $ASNLIB/publicdata/ $ASNLIB/publicdata/yelp_val.csv task2_2.csv
train_folder = sys.argv[1]
test_file = sys.argv[2]
output_file = sys.argv[3]
# train_folder = 'dataset/'
# test_file = 'dataset/yelp_val.csv'
# output_file = 'output/task2_2.csv'

conf = SparkConf().setAppName("INF553").setMaster('local[*]')
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

train_rdd = sc.textFile(os.path.join(train_folder, 'yelp_train.csv'))
train_header = train_rdd.first()
train_data = train_rdd.filter(lambda x: x != train_header).map(lambda x: x.split(",")).map(
    lambda x: (x[0], x[1], float(x[2])))

user_json_rdd = sc.textFile(os.path.join(train_folder, 'user.json')).map(json.loads)
user_json_map = user_json_rdd.map(
    lambda x: (x["user_id"], (x["review_count"], x["average_stars"], x["useful"], x["fans"]))).collectAsMap()
business_json_rdd = sc.textFile(os.path.join(train_folder, 'business.json')).map(json.loads)
business_json_map = business_json_rdd.map(
    lambda x: (x['business_id'], (x['stars'], x['review_count']))).collectAsMap()


# training phase - features : review_count, average_stars, useful, fans, stars, review_count. label : rating
training_data = []
label = []
for train_row in train_data.collect():
    training_data.append(get_feature_list(train_row))
    label.append(train_row[2])
training_data = np.asarray(training_data)
label = np.asarray(label)
train_data = xgb.DMatrix(training_data, label=label)
model = xgb.train(get_model_params(), train_data, 50)

test_rdd = sc.textFile(test_file)
test_rdd_header = test_rdd.first()
test_rdd = test_rdd.filter(lambda x: x != test_rdd_header)
test_data_val = test_rdd.map(lambda x: x.split(',')).map(lambda x: (x[0], x[1])).collect()

test_data = []
for test_row in test_data_val:
    test_data.append(get_feature_list(test_row))
test_data = np.asarray(test_data)
output = model.predict(xgb.DMatrix(test_data))
write_to_file(output_file, output, test_data_val)

# output_rdd = sc.textFile(output_file)
# output_header = output_rdd.first()
# output_data = output_rdd.filter(lambda x: x != output_header).map(lambda x: x.split(','))
# output_data_dict = output_data.map(lambda x: (((x[0]), (x[1])), float(x[2])))
# test_data_dict = test_rdd.map(lambda x: x.split(",")).map(lambda x: (((x[0]), (x[1])), float(x[2])))
# joined_data = test_data_dict.join(output_data_dict).map(lambda x: (abs(x[1][0] - x[1][1])))
#
# diff_0_to_1 = joined_data.filter(lambda x: x >= 0 and x < 1).count()
# diff_1_to_2 = joined_data.filter(lambda x: x >= 1 and x < 2).count()
# diff_2_to_3 = joined_data.filter(lambda x: x >= 2 and x < 3).count()
# diff_3_to_4 = joined_data.filter(lambda x: x >= 3 and x < 4).count()
# diff_more_than_4 = joined_data.filter(lambda x: x >= 4).count()
# print(">=0 and <1: ", diff_0_to_1)
# print(">=1 and <2: ", diff_1_to_2)
# print(">=2 and <3: ", diff_2_to_3)
# print(">=3 and <4: ", diff_3_to_4)
# print(">=4: ", diff_more_than_4)
# rmse_rdd = joined_data.map(lambda x: x ** 2).reduce(lambda x, y: x + y)
# rmse = math.sqrt(rmse_rdd / output_data_dict.count())
# print("RMSE", rmse)

print("Duration : ", time.time() - start_time)

# >=0 and <1 : 101714
# >=1 and <2 : 33288
# >=2 and <3 : 6222
# >=3 and <4 : 820
# >=4 :0
# RMSE 0.9833564690697524
# Duration: 56.854628801345825
#
# real 1m3.547s
# user 4m1.344s
# sys 0m4.896s
