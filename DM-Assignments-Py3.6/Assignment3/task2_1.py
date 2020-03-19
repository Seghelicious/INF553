import os
import sys
from pyspark import SparkContext
import time
import math

os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3.6"


def get_pearson_coefficient(test_data):
    user = test_data[0]
    business = test_data[1]
    user_rating_rdd = user_rating_rdd_broadcast.value
    business_rating_rdd = business_rating_rdd_broadcast.value
    user_avg_rating_rdd = user_avg_rating_rdd_broadcast.value
    business_avg_rating_rdd = business_avg_rating_rdd_broadcast.value
    neighbourhood_cutoff = 50
    if business not in business_rating_rdd:
        # Cold start (new business)
        if len(list(user_rating_rdd.get(user))) == 0:
            # user is new too
            return user, business, "2.5"
        return user, business, str(user_avg_rating_rdd.get(user))
    else:
        users_list = list(business_rating_rdd.get(business))
        average_business_rating = business_avg_rating_rdd.get(business)
        if user_rating_rdd.get(user) is None:
            # Cold start (new user)
            return user, business, str(average_business_rating)
        else:
            businesses_list = list(user_rating_rdd.get(user))  # list() gives list of keys in dict
            prediction_weight_sum = 0
            pearson_coefficient_sum = 0

            if len(businesses_list) != 0:  # user has given ratings
                pearson_coeff_and_rating = []
                for neighbour_business_id in businesses_list:
                    # sum_business_ratings = 0
                    # sum_neighbour_ratings = 0
                    user_index = 0
                    average_neighbour_rating = business_avg_rating_rdd.get(neighbour_business_id)
                    neighbour_business_ratings = business_rating_rdd.get(neighbour_business_id)
                    current_neighbour_rating = neighbour_business_ratings.get(user)
                    all_business_ratings = []
                    all_neighbour_ratings = []
                    while user_index < len(users_list):
                        current_user_id = users_list[user_index]
                        if neighbour_business_ratings.get(current_user_id):
                            business_rating = business_rating_rdd.get(business).get(current_user_id)
                            neighbour_rating = neighbour_business_ratings.get(current_user_id)
                            # sum_business_ratings += business_rating
                            # sum_neighbour_ratings += neighbour_rating
                            all_business_ratings.append(business_rating)
                            all_neighbour_ratings.append(neighbour_rating)
                        user_index += 1
                    if len(all_business_ratings) != 0:
                        # average_business_rating = sum_business_ratings / len(all_business_ratings)
                        # average_neighbour_rating = sum_neighbour_ratings / len(all_neighbours_ratings)
                        numerator = 0
                        denominator_business = 0
                        denominator_neighbour = 0
                        for j in range(0, len(all_business_ratings)):
                            normalized_business_rating = all_business_ratings[j] - average_business_rating
                            normalized_neighbour_rating = all_neighbour_ratings[j] - average_neighbour_rating
                            numerator += normalized_business_rating * normalized_neighbour_rating
                            denominator_business += pow(normalized_business_rating, 2)
                            denominator_neighbour += pow(normalized_neighbour_rating, 2)
                        denominator = math.sqrt(denominator_business) * math.sqrt(denominator_neighbour)
                        if denominator == 0:
                            if numerator == 0:
                                pearson_coefficient = 1
                            else:
                                continue
                        else:
                            pearson_coefficient = numerator / denominator
                        if pearson_coefficient > 1:
                            pearson_coefficient = 1
                        # print("pearson coefficient ", pearson_coefficient)
                        if pearson_coefficient > 0:
                            # pearson_coefficient = pearson_coefficient * pow(abs(pearson_coefficient), 1.5)  # case amplification
                            pearson_coeff_and_rating.append((pearson_coefficient, current_neighbour_rating))
                pearson_coeff_and_rating.sort(key=lambda x: x[0], reverse=True)
                if len(pearson_coeff_and_rating) == 0:
                    # no relation b/w businesses, returning avg of row and column ratings
                    return user, business, str((user_avg_rating_rdd.get(user) + business_avg_rating_rdd.get(business))/2)
                neighbourhood = min(len(pearson_coeff_and_rating), neighbourhood_cutoff)
                for x in range(neighbourhood):
                    prediction_weight_sum += pearson_coeff_and_rating[x][0] * pearson_coeff_and_rating[x][1]  # pearson_coeff * rating
                    pearson_coefficient_sum += abs(pearson_coeff_and_rating[x][0])

                if prediction_weight_sum == 0 or pearson_coefficient_sum == 0:
                    return user, business, str(average_business_rating)
                else:
                    prediction = prediction_weight_sum / pearson_coefficient_sum
                prediction = min(5.0, max(0.0, prediction))
                return user, business, str(prediction)
            else:
                # new user (no such user in yelp_test.csv)
                return user, business, str(average_business_rating)


start_time = time.time()

# time /home/local/spark/latest/bin/spark-submit task2_1.py $ASNLIB/publicdata/yelp_train.csv $ASNLIB/publicdata/yelp_val.csv task2-output.csv
# input_file_train = sys.argv[1]
# input_file_test = sys.argv[2]
# output_file = sys.argv[3]
input_file_train = 'dataset/yelp_train.csv'
input_file_test = 'dataset/yelp_val.csv'
output_file = 'output/task2.csv'

sc = SparkContext(appName="task2")
sc.setLogLevel("ERROR")
train_rdd = sc.textFile(input_file_train)
train_header = train_rdd.first()
train_data = train_rdd.filter(lambda x: x != train_header)

test_rdd = sc.textFile(input_file_test)
test_header = test_rdd.first()
test_data = test_rdd.filter(lambda x: x != test_header)
# test_data.collect()

test_matrix = test_data.map(lambda x: x.split(",")).sortBy(lambda x: ((x[0]), (x[1])))
user_rating_rdd = train_data.map(lambda x: x.split(',')).map(
    lambda x: ((x[0]), ((x[1]), float(x[2])))).groupByKey().sortByKey(True).mapValues(dict).collectAsMap()  # user key
business_rating_rdd = train_data.map(lambda x: x.split(',')).map(
    lambda x: ((x[1]), ((x[0]), float(x[2])))).groupByKey().sortByKey(True).mapValues(dict).collectAsMap()  # business key

user_avg_rating_rdd = train_data.map(lambda x: x.split(',')).map(
    lambda x: (x[0], float(x[2]))).groupByKey().mapValues(
    lambda x: sum(x) / len(x)).collectAsMap()  # userId <-> avg rating
business_avg_rating_rdd = train_data.map(lambda x: x.split(',')).map(
    lambda x: (x[1], float(x[2]))).groupByKey().mapValues(
    lambda x: sum(x) / len(x)).collectAsMap()  # businessId <-> avg rating

user_rating_rdd_broadcast = sc.broadcast(user_rating_rdd)
business_rating_rdd_broadcast = sc.broadcast(business_rating_rdd)
user_avg_rating_rdd_broadcast = sc.broadcast(user_avg_rating_rdd)
business_avg_rating_rdd_broadcast = sc.broadcast(business_avg_rating_rdd)

prediction_rdd = test_matrix.map(get_pearson_coefficient)
prediction_list = prediction_rdd.collect()

f = open(output_file, 'w')
f.write("user_id, business_id, prediction\n")
for i in range(len(prediction_list)):
    f.write(str(prediction_list[i][0]) + "," + str(prediction_list[i][1]) + "," + str(prediction_list[i][2]) + "\n")
f.close()

output_rdd = sc.textFile(output_file)
output_header = output_rdd.first()
output_data = output_rdd.filter(lambda x: x != output_header).map(lambda x: x.split(','))
output_data_dict = output_data.map(lambda x: (((x[0]), (x[1])), float(x[2])))
test_data_dict = test_data.map(lambda x: x.split(",")).map(lambda x: (((x[0]), (x[1])), float(x[2])))
joined_data = test_data_dict.join(output_data_dict).map(lambda x: (abs(x[1][0] - x[1][1])))

diff_0_to_1 = joined_data.filter(lambda x: x >= 0 and x < 1).count()
diff_1_to_2 = joined_data.filter(lambda x: x >= 1 and x < 2).count()
diff_2_to_3 = joined_data.filter(lambda x: x >= 2 and x < 3).count()
diff_3_to_4 = joined_data.filter(lambda x: x >= 3 and x < 4).count()
diff_more_than_4 = joined_data.filter(lambda x: x >= 4).count()
print(">=0 and <1: ", diff_0_to_1)
print(">=1 and <2: ", diff_1_to_2)
print(">=2 and <3: ", diff_2_to_3)
print(">=3 and <4: ", diff_3_to_4)
print(">=4: ", diff_more_than_4)
rmse_rdd = joined_data.map(lambda x: x ** 2).reduce(lambda x, y: x + y)
rmse = math.sqrt(rmse_rdd / output_data_dict.count())
print("RMSE", rmse)

print("Duration : ", time.time() - start_time)

# >=0 and <1:  91027
# >=1 and <2:  39565
# >=2 and <3:  8933
# >=3 and <4:  2161
# >=4:  358
# RMSE 1.1247146773328374
# Duration :  112.96282505989075
