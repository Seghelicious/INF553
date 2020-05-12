from pyspark import SparkConf, SparkContext
import time
import itertools
import random
import sys
import os

# os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3.6"
# export PYSPARK_PYTHON=python3.6


def write_to_file(data):
    with open(output_file, 'w') as file:
        file.write("business_id_1, business_id_2, similarity\n")
        for line in data.collect():
            file.write(str(line[0]) + "," + str(line[1]) + "," + str(line[2]) + "\n")


def create_hash_values(size):
    a = random.sample(range(1, 1000), size)
    b = random.sample(range(1, 1000), size)
    hash_values = []
    for i in range(size):
        hash_values.append([a[i], b[i]])
    return hash_values


def get_signature_matrix(x, hash_values, m):
    hashvalues = []
    a = hash_values[0]
    b = hash_values[1]
    # p = hash_values[2]
    for x in x[1]:
        value = (a*x + b) % m
        # value = ((a*x + b) % p) % m
        hashvalues.append(value)
    return min(hashvalues)


def lsh(x):
    band_data = []
    business = [x[0]]
    users = x[1]
    for band in range(bands):
        band_data.append(((band, tuple(users[band * row_size:(band + 1) * row_size])), business))
    return band_data


def pair_businesses(businesses):
    return sorted(list(itertools.combinations(sorted(businesses), 2)))


def jaccard_similarity(business1, business2):
    c1 = set(characteristic_matrix[business1])
    c2 = set(characteristic_matrix[business2])
    jaccard_sim = len(c1 & c2) / len(c1 | c2)
    return business1, business2, jaccard_sim


start_time = time.time()
input_file = 'dataset/yelp_train.csv'
output_file = 'output/task1.csv'
# input_file = sys.argv[1]
# output_file = sys.argv[2]


bands = 40
hash_values = create_hash_values(80)
row_size = int(len(hash_values) / bands)

conf = SparkConf().setAppName("INF553").setMaster('local[*]')
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
rdd = sc.textFile(input_file)
header = rdd.first()
data = rdd.filter(lambda x: x != header).map(lambda x: x.split(','))

users = data.map(lambda x: x[0]).distinct().collect()
users.sort()
user_to_number_dict = {}
for i, u in enumerate(users):
    user_to_number_dict[u] = i
num_users = len(user_to_number_dict)

matrix = data.map(lambda x: (x[1], user_to_number_dict[x[0]])).groupByKey().map(lambda x: (x[0], list(x[1]))).sortBy(lambda x: x[0])
characteristic_matrix = matrix.collectAsMap()

signature_matrix = matrix.map(lambda x: (x[0], [get_signature_matrix(x, hash_value_list, num_users) for hash_value_list in hash_values]))

similar_candidates = signature_matrix.flatMap(lsh).reduceByKey(lambda x, y: x + y).filter(lambda x: len(x[1]) > 1).flatMap(lambda x: pair_businesses(list(x[1]))).distinct()

result = similar_candidates.map(lambda business: jaccard_similarity(business[0], business[1])).filter(lambda x: x[2] >= 0.5).sortBy(lambda x: (x[0], x[1]))
write_to_file(result)

print("Duration: ", time.time() - start_time)
# Precision = 643/643 = 1
# Recall = 644/645 = 0.998
