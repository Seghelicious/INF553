from __future__ import print_function
import os
from pyspark import SparkContext
import time
import math

os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3.6"

# considering all ratings for finding average
def get_pearson_coefficient(user, business):
    business_rating_rdd = business_rating_rdd_broadcast.value
    user_rating_rdd = user_rating_rdd_broadcast.value
    neighbourhood_cutoff = 9
    if business in business_rating_rdd:
        users_list = list(business_rating_rdd.get(business))  # list() gives list of keys in dict
        business_ratings = business_rating_rdd.get(business)
        sum_business_rating = sum(business_ratings.values())
        average_business_rating = sum_business_rating / len(business_ratings)

        if user_rating_rdd.get(user) is None:
            #new user
            return user, business, str(average_business_rating)
        else:
            businesses_list = list(user_rating_rdd.get(user))  # list() gives list of keys in dict
            prediction_weight_sum = 0
            pearson_coefficient_sum = 0

            # create dict of businessId <-> average


            if len(businesses_list) != 0:  # old user
                avg_dict = {}
                for i in range(len(businesses_list)):
                    business_id = businesses_list[i]
                    neighbour_business_ratings = business_rating_rdd[business_id]
                    sum_ratings = 0
                    rating_count = 0
                    user_index = 0
                    if neighbour_business_ratings.get(users_list[user_index]):
                        sum_ratings += neighbour_business_ratings.get(users_list[user_index])
                        rating_count += 1
                        user_index += 1
                    overall_average_rating = 0
                    if rating_count != 0:
                        overall_average_rating = sum_ratings / rating_count
                    avg_dict[business_id] = overall_average_rating

                pearson_coeff_and_rating = []
                for i in range(len(businesses_list)):  # NOTE:  only do for some neighbours with highest pearson
                    # sum_business_ratings = 0
                    # sum_neighbour_ratings = 0
                    user_index = 0
                    neighbour_business_ratings = business_rating_rdd[business_id]
                    current_neighbour_rating = neighbour_business_ratings.get(user)
                    all_business_ratings = []
                    all_neighbours_ratings = []
                    count = 0
                    while user_index < len(users_list):
                        if neighbour_business_ratings.get(users_list[user_index]):
                            # sum_business_ratings += business_rating_rdd[business].get(users_list[user_index])
                            # sum_neighbour_ratings += neighbour_business_ratings.get(users_list[user_index])
                            all_business_ratings.append(business_rating_rdd[business].get(users_list[user_index]))
                            all_neighbours_ratings.append(neighbour_business_ratings.get(users_list[user_index]))
                        user_index += 1
                    if len(all_neighbours_ratings) != 0:
                        # average_business_rating = sum_business_ratings / len(all_business_ratings)
                        # average_business_rating = average_business_rating
                        average_neighbour_rating = avg_dict[business_id] / len(all_neighbours_ratings)
                        numerator = 0
                        denominator_business = 0
                        denominator_neighbour = 0
                        for j in range(len(all_business_ratings)):
                            normalized_business_rating = all_business_ratings[j] - average_business_rating
                            normalized_neighbour_rating = all_neighbours_ratings[j] - average_neighbour_rating
                            numerator += normalized_business_rating * normalized_neighbour_rating
                            denominator_business += pow(normalized_business_rating, 2)
                            denominator_neighbour += pow(normalized_neighbour_rating, 2)
                        denominator = math.sqrt(denominator_business) * math.sqrt(denominator_neighbour)
                        pearson_coefficient = 0
                        if denominator != 0:
                            pearson_coefficient = numerator / denominator
                        pearson_coeff_and_rating.append((pearson_coefficient, current_neighbour_rating))
                pearson_coeff_and_rating.sort(key=lambda x: x[0], reverse=True)
                neighbourhood = len(pearson_coeff_and_rating)
                if neighbourhood > neighbourhood_cutoff:
                    neighbourhood = neighbourhood_cutoff
                for x in range(neighbourhood):
                    prediction_weight_sum += pearson_coeff_and_rating[x][0] * pearson_coeff_and_rating[x][1]  # pearson_coeff * rating
                    pearson_coefficient_sum += abs(pearson_coeff_and_rating[x][0])

                if pearson_coefficient_sum == 0:
                    return user, business, str(average_business_rating)
                prediction = prediction_weight_sum / pearson_coefficient_sum
                if prediction < 0:
                    prediction = 0.0
                elif prediction > 5:
                    prediction = 5.0
                return user, business, str(prediction)
            else:
                #new user
                return user, business, "2.7"
    else:
        #new business
        return user, business, "2.7"


start_time = time.time()
input_train = 'dataset/yelp_train.csv'
input_test = 'dataset/yelp_val.csv'
output_file = 'output/task2_bkp.csv'

sc = SparkContext(appName="task2")

train_rdd = sc.textFile(input_train)
train_header = train_rdd.first()
train_data = train_rdd.filter(lambda x: x != train_header)

test_rdd = sc.textFile(input_test)
test_header = test_rdd.first()
test_data = test_rdd.filter(lambda x: x != train_header)
test_data.collect()

test_matrix = test_data.map(lambda x: x.split(",")).sortBy(lambda x: ((x[0]), (x[1])))
train_matrix = train_data.map(lambda x: x.split(',')).map(lambda x: ((x[0]), ((x[1]), float(x[2])))).groupByKey().sortByKey(True)
d_user_rating_rdd = train_matrix.mapValues(dict).collectAsMap()  # user as key

rating_rdd_b = train_data.map(lambda x: x.split(',')).map(lambda x: ((x[1]), ((x[0]), float(x[2])))).groupByKey().sortByKey(True)
d_business_rating_rdd = rating_rdd_b.mapValues(dict).collectAsMap()  # Business as key

user_rating_rdd_broadcast = sc.broadcast(d_user_rating_rdd)
business_rating_rdd_broadcast = sc.broadcast(d_business_rating_rdd)

prediction_rdd = test_matrix.map(lambda x: get_pearson_coefficient(x[0], x[1]))
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
joined_data = test_data_dict.join(output_data_dict).map(lambda x: (((x[0]), (x[1])), abs(x[1][0] - x[1][1])))
joined_data_list = joined_data.collect()
g0l1 = 0
g1l2 = 0
g2l3 = 0
g3l4 = 0
g4 = 0
for i in range(len(joined_data_list)):
    if joined_data_list[i][1] >= 0 and joined_data_list[i][1] < 1:
        g0l1 += 1
    elif (joined_data_list[i][1] >= 1 and joined_data_list[i][1] < 2):
        g1l2 += 1
    elif (joined_data_list[i][1] >= 2 and joined_data_list[i][1] < 3):
        g2l3 += 1
    elif (joined_data_list[i][1] >= 3 and joined_data_list[i][1] < 4):
        g3l4 += 1
    elif (joined_data_list[i][1] >= 4):
        g4 += 1

# for i in range(0, len)
print(">=0 and <1:", g0l1)
print(">=1 and <2:", g1l2)
print(">=2 and <3:", g2l3)
print(">=3 and <4:", g3l4)
print(">=", g4)
rdd1 = joined_data.map(lambda x: x[1] ** 2).reduce(lambda x, y: x + y)
rmse = math.sqrt(rdd1 / output_data_dict.count())
print("RMSE: ", rmse)
time2 = time.time()
print("Time: ", time2 - start_time)

