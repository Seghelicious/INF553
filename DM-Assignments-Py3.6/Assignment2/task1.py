from pyspark import SparkConf, SparkContext
import sys
import itertools
import time
import os

#os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3.6"
#export PYSPARK_PYTHON=python3.6


def create_count_dict(items, support_value):
    item_counts = {}
    for item in items:
        if item not in item_counts.keys():
            item_counts[item] = 1
        else:
            if item_counts[item] < support_value:
                item_counts[item] = item_counts[item] + 1
    return item_counts


def create_count_dict_tuple(chunk, support_value, candidate_tuples):
    tuple_counts = {}
    for basket in chunk:
        for candidate in candidate_tuples:
            if set(candidate).issubset(basket):
                if candidate in tuple_counts and tuple_counts[candidate] < support_value:
                    tuple_counts[candidate] += 1
                elif candidate not in tuple_counts:
                    tuple_counts[candidate] = 1
    return tuple_counts


def filter_by_support(count_dict, support_value, is_tuple):
    frequent_candidates = []
    for candidate, count in count_dict.items():
        if count >= support_value:
            frequent_candidates.append(candidate)
            phase1_candidates.append((candidate if is_tuple else tuple({candidate}), 1))
    return frequent_candidates


def get_frequent_candidate_tuples(frequent_items, size):
    candidates = []
    for item_x in frequent_items:
        for item_y in frequent_items:
            combined_set = tuple(sorted(set(item_x + item_y)))
            if len(combined_set) == size:
                if combined_set not in candidates:
                    previous_candidates = list(itertools.combinations(combined_set, size - 1))
                    flag = True
                    for candidate in previous_candidates:
                        if candidate not in frequent_items:
                            flag = False
                            break
                    if flag:
                        candidates.append(combined_set)
    return candidates


def find_frequent_candidates(chunk, chunk_support, frequent_items, size):
    if size == 2:
        candidates = list(itertools.combinations(sorted(frequent_items), 2))
    else:
        candidates = get_frequent_candidate_tuples(frequent_items, size)
    return filter_by_support(create_count_dict_tuple(chunk, chunk_support, candidates), chunk_support, True)


def apriori(baskets):
    chunk = list(baskets)
    chunk_support = support * (len(chunk) / basket_count)
    items = []
    for basket in chunk:
        for item in basket:
            items.append(item)

    #frequent itemsets of size 1
    frequent_items = filter_by_support(create_count_dict(items, chunk_support), chunk_support, False)
    size = 2
    while len(frequent_items) != 0:
        frequent_items = find_frequent_candidates(chunk, chunk_support, frequent_items, size)
        size += 1

    return [phase1_candidates]


def son(candidate):
    frequent_itemset_count = {}
    for basket in basket_list:
        if isinstance(candidate, tuple):
            if set(candidate).issubset(basket):
                if candidate in frequent_itemset_count and frequent_itemset_count[candidate] < support:
                    frequent_itemset_count[candidate] += 1
                elif candidate not in frequent_itemset_count:
                    frequent_itemset_count[candidate] = 1
        else:
            frequent_itemset_count = create_count_dict(basket, support)

    frequent_itemsets_actual = []
    for itemset, count in frequent_itemset_count.items():
        frequent_itemsets_actual.append((itemset, count))
    return frequent_itemsets_actual


def get_output_string(input_tuple, size, output):
    if len(input_tuple) > size:
        output = output[:-1] + "\n\n"

    if len(input_tuple) == 1:
        output = output + "('" + str(input_tuple[0]) + "'),"
    else:
        output = output + str(input_tuple) + ","
    return output


def get_output(phase_rdd, is_phase_2):
    size = 1
    output = ""
    sorted_list = sorted(sorted(phase_rdd if is_phase_2 else phase_rdd.map(lambda x: x[0]).collect()), key=len)
    for x in sorted_list:
        output = get_output_string(x, size, output)
        size = len(x)
    return output


def get_required_bucket(row, case):
    line = row.split(",")
    if case == 1:
        return line[0], line[1]
    elif case == 2:
        return line[1], line[0]


def write_to_file(candidates, frequent_itemsets):
    with open(output_file, 'w') as file:
        file.write("Candidates:\n")
        file.write(candidates + "\n\n")
        file.write("Frequent Itemsets:\n")
        file.write(frequent_itemsets)


start_time = time.time()
case = int(sys.argv[1])
support = int(sys.argv[2])
input_file = sys.argv[3]
output_file = sys.argv[4]
phase1_candidates = []

conf = SparkConf().setAppName("INF553").setMaster('local[*]')
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
rdd = sc.textFile(input_file)
column_names = rdd.first()

basket_rdd = rdd.filter(lambda x: x != column_names).map(lambda x: get_required_bucket(x, case)).groupByKey().mapValues(
    set).map(lambda x: x[1]).persist()
basket_list = basket_rdd.collect()
basket_count = len(basket_list)

phase1_map = basket_rdd.mapPartitions(apriori).flatMap(lambda x: x)
phase1_reduce = phase1_map.reduceByKey(lambda x, y: x + y).persist()

phase2_map = phase1_reduce.map(lambda x: son(x[0])).flatMap(lambda x: x)
phase2_reduce = phase2_map.filter(lambda x: x[1] >= support).map(lambda x: x[0]).collect()

phase1_output = get_output(phase1_reduce, False)[:-1]
phase2_output = get_output(phase2_reduce, True)[:-1]

sc.stop()
write_to_file(phase1_output, phase2_output)

print("Duration:", time.time() - start_time)
