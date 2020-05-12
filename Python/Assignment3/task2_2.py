import os
import sys
import json
import numpy as np
import pandas as pd
import xgboost as xgb
from pyspark import SparkContext, SparkConf
import time

from sklearn import preprocessing
from sklearn.metrics import mean_squared_error


def get_features(data, user_json_map, business_json_map):
    user_review_count = []
    user_review_useful = []
    user_fans = []
    user_avg_rating = []
    business_avg_rating = []
    business_review_count = []
    business_price_range = []

    for user_id in data['user_id']:
        if user_id in user_json_map.keys():
            user_review_count.append(user_json_map.get(user_id)[0])
            user_review_useful.append(user_json_map.get(user_id)[1])
            user_fans.append(user_json_map.get(user_id)[2])
            user_avg_rating.append(user_json_map.get(user_id)[3])
        else:
            user_review_count.append(avg_users_reviewcount)
            user_review_useful.append(0)
            user_fans.append(0)
            user_avg_rating.append(avg_users_rating)

    for business_id in data['business_id']:
        if business_id in business_json_map.keys():
            business_avg_rating.append(business_json_map.get(business_id)[0])
            business_review_count.append(business_json_map.get(business_id)[1])
            business_price_range.append(business_json_map.get(business_id)[2])
        else:
            business_avg_rating.append(avg_businesses_rating)
            business_review_count.append(avg_businesses_reviewcount)
            business_price_range.append(2)

    data['user_review_count'] = user_review_count
    data['user_review_useful'] = user_review_useful
    data['user_fans'] = user_fans
    data['user_avg_rating'] = user_avg_rating
    data['business_avg_rating'] = business_avg_rating
    data['business_review_count'] = business_review_count
    data['business_price_range'] = business_price_range
    return data


def fit_invalid_type_cols(features):
    for col in features.columns:
        if features[col].dtype == 'object':
            label = preprocessing.LabelEncoder()
            label.fit(list(features[col].values))
            features[col] = label.transform(list(features[col].values))
    return features


def get_price_range(attributes, key):
    if attributes:
        if key in attributes.keys():
            return int(attributes.get(key))
    return 0


def write_to_file(df, output_file):
    df.to_csv(output_file, header=['user_id', ' business_id', ' prediction'], index=False, sep=',', mode='w')


start_time = time.time()
# time /home/local/spark/latest/bin/spark-submit task2_2.py $ASNLIB/publicdata/ $ASNLIB/publicdata/yelp_val.csv task2_2.csv
train_folder = sys.argv[1]
test_file = sys.argv[2]
output_file = sys.argv[3]
# train_folder = 'dataset/'
# test_file = 'dataset/yelp_val.csv'
# output_file = 'output/task2_2.csv'

train_data = pd.read_csv(os.path.join(train_folder, 'yelp_train.csv'))
test_data = pd.read_csv(test_file)
test_copy = test_data.copy()

conf = SparkConf().setAppName("INF553").setMaster('local[*]')
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

user_json_rdd = sc.textFile(os.path.join(train_folder, 'user.json')).map(json.loads).map(
    lambda x: ((x["user_id"]), (x["review_count"], x["useful"], x["fans"], x["average_stars"]))).persist()
user_json_map = user_json_rdd.collectAsMap()

business_json_rdd = sc.textFile(os.path.join(train_folder, 'business.json')).map(json.loads).map(
    lambda x: ((x['business_id']), (x['stars'], x['review_count'], get_price_range(x['attributes'], 'RestaurantsPriceRange2')))).persist()
business_json_map = business_json_rdd.collectAsMap()

avg_users_rating = user_json_rdd.map(lambda x: x[1][3]).mean()
avg_users_reviewcount = user_json_rdd.map(lambda x: x[1][0]).mean()
avg_businesses_rating = business_json_rdd.map(lambda x: x[1][0]).mean()
avg_businesses_reviewcount = business_json_rdd.map(lambda x: x[1][1]).mean()

train_features = get_features(train_data, user_json_map, business_json_map)
train_features = fit_invalid_type_cols(train_features)
x_train = train_features.drop(["stars"], axis=1)
y_train = train_features.stars.values

model = xgb.XGBRegressor(learning_rate=0.3)
model.fit(x_train, y_train)

test_features = get_features(test_copy, user_json_map, business_json_map)
test_features = fit_invalid_type_cols(test_features)
x_test = test_features.drop(["stars"], axis=1)
prediction = model.predict(data=x_test)

result = pd.DataFrame()
result["user_id"] = test_data.user_id.values
result["business_id"] = test_data.business_id.values
result["prediction"] = prediction

write_to_file(result, output_file)

# print("RMSE : ", np.sqrt(mean_squared_error(test_data.stars.values, prediction)))
print("Duration: ", time.time() - start_time)

# RMSE :  0.9838945490587362
# Duration:  42.468539237976074
