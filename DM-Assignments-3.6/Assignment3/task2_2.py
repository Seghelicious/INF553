import os
import sys
import json
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.datasets import load_boston
from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_val_score, KFold
from sklearn.metrics import mean_squared_error
from pyspark import SparkContext, SparkConf
import time

os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3.6"
# export PYSPARK_PYTHON=python3.6

start_time = time.time()

# time /home/local/spark/latest/bin/spark-submit task2_1.py $ASNLIB/publicdata/yelp_train.csv $ASNLIB/publicdata/yelp_val.csv task2-output.csv
# train_folder_path = sys.argv[1]
# input_file_test = sys.argv[2]
# output_file = sys.argv[3]
train_folder = 'dataset/'
input_file_test = 'dataset/yelp_val.csv'
output_file = 'output/task2.csv'


conf = SparkConf().setAppName("INF553").setMaster('local[*]')
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")











conf = SparkConf().setAppName("INF553").setMaster('local[*]')
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
rdd_user = sc.textFile(train_folder + "/user.json").map(lambda x: json.loads(x))
rdd_user = rdd_user.repartition(8).map(lambda x: (x["user_id"], (x["review_count"], x["average_stars"]))).persist()

rdd_train = sc.textFile(train_folder + "yelp_train.csv")
train_header = rdd_train.first()
pd_train_data = rdd_train.filter(lambda x: x != train_header).map(lambda x: x.split(',')).map(
    lambda x: ((x[0]), (x[1], float(x[2])))).persist()  # user key

rdd_test = sc.textFile(input_file_test)
test_header = rdd_test.first()
test_data = rdd_test.filter(lambda x: x != test_header).map(lambda x: x.split(',')).map(
    lambda x: ((x[0]), (x[1], float(x[2])))).persist()  # user key

# userid, businessid, review_count_by_user, avg_rating_by_user, rating
intermediate_train_rdd = pd_train_data.join(rdd_user).map(lambda x: (x[0], x[1][0][0], x[1][1][0], x[1][1][1], x[1][0][1]))
intermediate_test_rdd = test_data.join(rdd_user).map(lambda x: (x[0], x[1][0][0], x[1][1][0], x[1][1][1]))

max_depth = 3
min_child_weight = 10
subsample = 0.5
colsample_bytree = 0.6
objective = 'reg:linear'
num_estimators = 1000
learning_rate = 0.3

clf = xgb.XGBRegressor(max_depth=max_depth,
                       min_child_weight=min_child_weight,
                       subsample=subsample,
                       colsample_bytree=colsample_bytree,
                       objective=objective,
                       n_estimators=num_estimators,
                       learning_rate=learning_rate)
clf.fit(intermediate_train_rdd, intermediate_test_rdd)
