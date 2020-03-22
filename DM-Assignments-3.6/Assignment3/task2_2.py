import os
import sys
import json
import numpy as np
import pandas as pd
import xgboost as xgb
from pandas import json_normalize
from pyspark import SparkContext, SparkConf
import time

os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3.6"

start_time = time.time()
# time /home/local/spark/latest/bin/spark-submit task2_1.py $ASNLIB/publicdata/yelp_train.csv $ASNLIB/publicdata/yelp_val.csv task2-output.csv
# train_folder_path = sys.argv[1]
# input_file_test = sys.argv[2]
# output_file = sys.argv[3]
train_folder = 'dataset/'
test_file = 'dataset/yelp_val.csv'
output_file = 'output/task2_2.csv'





conf = SparkConf().setAppName("INF553").setMaster('local[*]')
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")


user_file = 'dataset/user.json'
business_file = 'dataset/business.json'
yelp_train = 'dataset/yelp_train.csv'








train_data = pd.read_csv(os.path.join(train_folder, 'yelp_train.csv'))
test_data = pd.read_csv(test_file)

train_user_ratings = pd.DataFrame(train_data.groupby(['user_id'])['stars'].mean())
train_user_ratings['user_rating_count'] = train_data.groupby('user_id')['stars'].count()
train_user_ratings = train_user_ratings.reset_index()
train_user_ratings = train_user_ratings.rename(columns={"stars": "user_average_rating"})

# Loading user.json to dataframe (Features : review_count, average_stars, useful)
# user_json_map = sc.textFile(os.path.join(train_folder, 'user.json')).map(json.loads)
# user_json_map = user_json_map.map(
#     lambda x: (x["user_id"], x["review_count"], x["average_stars"], x["useful"], x["fans"])).collectAsMap()
# business_json_map = sc.textFile(os.path.join(train_folder, 'user.json')).map(json.loads)
# business_json_map = business_json_map.map(
#     lambda x: (x['business_id'], [x['stars'], x['review_count']])).collectAsMap()




df_user_json = user_json_map.collect()
df_user_json = pd.DataFrame(df_user_json)
df_user_json.to_csv('user_features.csv', index=False)  # writing to csv for dataframe load
df_user_json = pd.read_csv('user_features.csv')
df_user_json = df_user_json.rename(
    columns={'0': "user_json_userid", '1': "user_json_reviewcount", '2': "user_json_averagestars",
             '3': "user_json_useful", '4': "user_json_fans"})

# Loading business.json to dataframe (Features : review_count, stars, is_open)

business_data = pd.read_json(os.path.join(train_folder, 'business.json'), lines=True, orient='records')
df_business_json = pd.DataFrame(business_data)
# df_business_json = json_normalize(df_business_json.to_dict())


# Joining yelp_train, with yelp_train(with average stars and review count), user.json and business.json
df_train = train_data.join(train_user_ratings, lsuffix='_train', rsuffix='_train_usersummary', how='inner')
df_train = df_train.join(df_business_json, lsuffix='_train', rsuffix='_businessjson', how='inner')
df_train = df_train.join(df_user_json, lsuffix='_train', rsuffix='_userjson', how='inner')

df_test = test_data.join(train_user_ratings, lsuffix='_test', rsuffix='_train_usersummary', how='left')
df_test = df_test.join(df_business_json, lsuffix='_test', rsuffix='_businessjson', how='left', )
df_test = df_test.join(df_user_json, lsuffix='_test', rsuffix='_userjson', how='left')

df_test['user_average_rating'].fillna(np.nanmean(df_test['user_average_rating']))
df_test['user_rating_count'].fillna(np.nanmean(df_test['user_rating_count']))

X_train = df_train[
    ['user_average_rating', 'user_rating_count', 'review_count', 'is_open',
     'user_json_reviewcount', 'user_json_averagestars', 'user_json_useful', 'user_json_fans']]
Y_train = df_train['stars']

model = xgb.XGBRegressor(label=0.5, eval_metric="auc", booster="gbtree", objective="binary:logistic", eta=0.0202048, max_depth=5, subsample = 0.6815, colsample_bytree = 0.701, silent = 1)
model.fit(X_train, Y_train)

X_test = df_test[
    ['user_average_rating', 'user_rating_count', 'review_count', 'is_open',
     'user_json_reviewcount', 'user_json_averagestars', 'user_json_useful', 'user_json_fans']]
output = model.predict(data=X_test)

result_df = pd.DataFrame()
result_df['user_id'] = df_test['user_id_test']
result_df['business_id'] = df_test['business_id_test']
result_df['prediction'] = pd.Series(output)
result_df['rating'] = df_test['stars']

result_df.to_csv(output_file, encoding='utf-8', index=False)

print(((result_df.prediction - result_df.rating) ** 2).mean() ** .5)
print("Duration : ", time.time() - start_time)
