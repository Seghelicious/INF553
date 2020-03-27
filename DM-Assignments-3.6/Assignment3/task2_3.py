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


def get_price_range(attributes, key):
    if attributes:
        if key in attributes.keys():
            return int(attributes.get(key))
    return 0


def write_to_file_model_based(output_file, output, test_data):
    file = open(output_file, 'w')
    file.write("user_id, business_id, prediction\n")
    for i in range(0, len(output)):
        file.write(test_data[i][0] + "," + test_data[i][1] + "," + str( max(1, min(5, output[i]))) + "\n")


def write_to_file(output_file, prediction_list):
    f = open(output_file, 'w')
    f.write("user_id, business_id, prediction\n")
    for i in range(len(prediction_list)):
        f.write(str(prediction_list[i][0][0]) + "," + str(prediction_list[i][0][1]) + "," + str(prediction_list[i][1]) + "\n")
    f.close()


def get_pearson_coefficient(neighbour_id, users_list, business_ratings, average_business_rating):
    average_neighbour_rating = business_avg_rating_map.get(neighbour_id)
    neighbour_business_ratings = business_rating_map.get(neighbour_id)
    all_business_ratings = []
    all_neighbour_ratings = []
    for current_user_id in users_list:
        if neighbour_business_ratings.get(current_user_id):
            business_rating = business_ratings.get(current_user_id)
            neighbour_rating = neighbour_business_ratings.get(current_user_id)
            all_business_ratings.append(business_rating)
            all_neighbour_ratings.append(neighbour_rating)
    if len(all_business_ratings) != 0:
        numerator = 0
        denominator_business = 0
        denominator_neighbour = 0
        for j in range(0, len(all_business_ratings)):
            normalized_business_rating = all_business_ratings[j] - average_business_rating
            normalized_neighbour_rating = all_neighbour_ratings[j] - average_neighbour_rating
            numerator += normalized_business_rating * normalized_neighbour_rating
            denominator_business += normalized_business_rating * normalized_business_rating
            denominator_neighbour += normalized_neighbour_rating * normalized_neighbour_rating
        denominator = math.sqrt(denominator_business * denominator_neighbour)
        if denominator == 0:
            if numerator == 0:
                pearson_coefficient = 1
            else:
                return -1
        else:
            pearson_coefficient = numerator / denominator
    else:
        pearson_coefficient = float(average_business_rating / average_neighbour_rating)
    return pearson_coefficient


def get_prediction(pearson_coeff_and_rating_list, default_average):
    prediction_weight_sum = 0
    pearson_coefficient_sum = 0
    neighbourhood_cutoff = 50
    pearson_coeff_and_rating_list.sort(key=lambda x: x[0], reverse=True)
    if len(pearson_coeff_and_rating_list) == 0:
        # couldnt get valid pearson coeff b/w businesses, returning avg of avg_user_rating and avg_business_rating
        return default_average
    neighbourhood = min(len(pearson_coeff_and_rating_list), neighbourhood_cutoff)
    for x in range(neighbourhood):
        prediction_weight_sum += pearson_coeff_and_rating_list[x][0] * pearson_coeff_and_rating_list[x][
            1]  # pearson_coeff * rating
        pearson_coefficient_sum += abs(pearson_coeff_and_rating_list[x][0])
    prediction = prediction_weight_sum / pearson_coefficient_sum
    return min(5.0, max(0.0, prediction))


def item_based_prediction(test_data):
    user = test_data[0]
    business = test_data[1]
    if business not in business_rating_map:
        # Cold start (new business)
        if len(list(user_rating_map.get(user))) == 0:
            # user is new too
            return user, business, "2.5"
        return user, business, str(user_avg_rating_map.get(user))
    else:
        users_list = list(business_rating_map.get(business))
        business_ratings = business_rating_map.get(business)
        average_business_rating = business_avg_rating_map.get(business)
        if user_rating_map.get(user) is None:
            # Cold start (new user)
            return user, business, str(average_business_rating)
        else:
            businesses_list = list(user_rating_map.get(user))  # list() gives list of keys in dict
            if len(businesses_list) != 0:  # user has given ratings
                pearson_coeff_and_rating_list = []
                for neighbour_business_id in businesses_list:
                    current_neighbour_rating = business_rating_map.get(neighbour_business_id).get(user)
                    pearson_coefficient = get_pearson_coefficient(neighbour_business_id, users_list, business_ratings, average_business_rating)
                    if pearson_coefficient > 0:
                        if pearson_coefficient > 1:
                            pearson_coefficient = 1 / pearson_coefficient
                        pearson_coeff_and_rating_list.append((pearson_coefficient, current_neighbour_rating))
                prediction = get_prediction(pearson_coeff_and_rating_list, (user_avg_rating_map.get(user) + average_business_rating) / 2)
                return user, business, min(5.0, max(0.0, prediction))
            else:
                # new user (no such user in yelp_test.csv)
                return user, business, str(average_business_rating)


start_time = time.time()

# time /home/local/spark/latest/bin/spark-submit task2_3.py $ASNLIB/publicdata/ $ASNLIB/publicdata/yelp_val.csv task2_3.csv
train_folder = sys.argv[1]
test_file = sys.argv[2]
output_file = sys.argv[3]
# train_folder = 'dataset/'
# test_file = 'dataset/yelp_val.csv'
# output_file = 'output/task2_3.csv'

conf = SparkConf().setAppName("INF553").setMaster('local[*]')
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
train_rdd = sc.textFile(os.path.join(train_folder, 'yelp_train.csv'))
train_header = train_rdd.first()
train_data = train_rdd.filter(lambda x: x != train_header).map(lambda x: x.split(','))

test_rdd = sc.textFile(test_file)
test_header = test_rdd.first()
test_rdd = test_rdd.filter(lambda x: x != test_header)


# ITEM BASED RECOMMENDATION
user_rating_map = train_data.map(lambda x: ((x[0]), ((x[1]), float(x[2])))).groupByKey().sortByKey(True).mapValues(dict).collectAsMap()  # user key
business_rating_map = train_data.map(lambda x: ((x[1]), ((x[0]), float(x[2])))).groupByKey().sortByKey(True).mapValues(dict).collectAsMap()  # business key
user_avg_rating_map = train_data.map(lambda x: (x[0], float(x[2]))).groupByKey().mapValues(lambda x: sum(x) / len(x)).collectAsMap()  # userId <-> avg rating
business_avg_rating_map = train_data.map(lambda x: (x[1], float(x[2]))).groupByKey().mapValues(lambda x: sum(x) / len(x)).collectAsMap()  # businessId <-> avg rating
test_matrix = test_rdd.map(lambda x: x.split(",")).sortBy(lambda x: ((x[0]), (x[1]))).persist()
prediction_item_based = test_matrix.map(item_based_prediction).map(lambda x: ((x[0], x[1]), float(x[2])))
# print("itembased done")

# MODEL BASED RECOMMENDATION
user_json_map = sc.textFile(os.path.join(train_folder, 'user.json')).map(json.loads).map(
    lambda x: ((x["user_id"]), (x["review_count"], x["useful"], x["fans"], x["average_stars"]))).collectAsMap()
business_json_map = sc.textFile(os.path.join(train_folder, 'business.json')).map(json.loads).map(
    lambda x: ((x['business_id']), (x['stars'], x['review_count'], get_price_range(x['attributes'], 'RestaurantsPriceRange2')))).collectAsMap()
training_data = []
label = []
for train_row in train_data.collect():
    training_data.append(get_feature_list(train_row))
    label.append(train_row[2])
training_data = np.asarray(training_data)
label = np.asarray(label)
trained_data = xgb.DMatrix(training_data, label=label)
model = xgb.train(get_model_params(), trained_data, 100)

test_data_val = test_rdd.map(lambda x: x.split(',')).map(lambda x: (x[0], x[1])).collect()

test_data = []
for test_row in test_data_val:
    test_data.append(get_feature_list(test_row))
test_data = np.asarray(test_data)
prediction = model.predict(xgb.DMatrix(test_data))
temp_file = "model_based_temp_output.csv"
write_to_file_model_based(temp_file, prediction, test_data_val)

output_rdd = sc.textFile(temp_file)
output_header = output_rdd.first()
output_data = output_rdd.filter(lambda x: x != output_header).map(lambda x: x.split(','))
prediction_model_based = output_data.map(lambda x: (((x[0]), (x[1])), float(x[2])))
# print("modelbased done")

# HYBRID BASED RECOMMENDATION
output_hybrid = prediction_item_based.join(prediction_model_based).map(lambda x: ((x[0]), float((x[1][0] * 0.1 + x[1][1] * 0.9))))
write_to_file(output_file, output_hybrid.collect())
# print("hybridbased done")


# test_data_dict = test_rdd.map(lambda x: x.split(",")).map(lambda x: (((x[0]), (x[1])), float(x[2])))
# joined_data = test_data_dict.join(output_hybrid).map(lambda x: (abs(x[1][0] - x[1][1])))
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
# rmse = math.sqrt(rmse_rdd / output_hybrid.count())
# print("RMSE", rmse)

print("Duration : ", time.time() - start_time)

# >=0 and <1:  101667
# >=1 and <2:  33352
# >=2 and <3:  6222
# >=3 and <4:  803
# >=4:  0
# RMSE 0.983011640665242
# Duration :  123.34113597869873
