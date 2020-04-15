from blackbox import BlackBox
import binascii
import random
import sys
import time


# a = [15, 33, 124, 234, 345, 423, 543, 163, 867, 945]
# b = [49, 345, 5674, 1929, 8324, 3728, 9473, 7827, 6435, 8273]
# p = [1543, 6151, 18583, 20249, 23887, 39113, 241867, 375979, 541859, 942217]


def create_hash_functions(n):
    a = random.sample(range(1, 1000), n)
    b = random.sample(range(1, 1000), n)
    p = random.sample(range(10000, 10000000), n)
    hash_values = []
    for i in range(n):
        hash_values.append([a[i], b[i], p[i]])
    return hash_values


def myhashs(s):
    result = []
    user_int = int(binascii.hexlify(s.encode('utf8')), 16)
    for h in hash_functions:
        result.append(((h[0] * user_int + h[1]) % h[2]) % m)
    return result


def bloom_filter(stream_users, ask):
    global global_user_set, filter_bit_array
    false_positives = 0
    true_negatives = 0
    for user_id in stream_users:
        is_new_user = False
        hash_values = myhashs(user_id)
        for hash_value in hash_values:
            if filter_bit_array[hash_value] == 0:
                filter_bit_array[hash_value] = 1
                is_new_user = True
        if user_id not in global_user_set:
            if is_new_user:
                true_negatives += 1  # True nagative: x not in S, and identified as not in S
            else:
                false_positives += 1  # False positive: x not in S, but identified as in S
        global_user_set.add(user_id)
    if false_positives == 0 and true_negatives == 0:
        fpr = 0.0
    else:
        fpr = false_positives / float(false_positives + true_negatives)
    f.write(str(ask) + "," + str(fpr) + "\n")


start_time = time.time()

# input_file = 'dataset/users.txt'
# stream_size = 500
# num_of_asks = 30
# output_file = 'output/task1.csv'

input_file = sys.argv[1]
stream_size = int(sys.argv[2])
num_of_asks = int(sys.argv[3])
output_file = sys.argv[4]

m = 69997
filter_bit_array = [0] * m
global_user_set = set()
hash_functions = create_hash_functions(15)

f = open(output_file, "w")
f.write("Time,FPR\n")

bx = BlackBox()
for ask in range(num_of_asks):
    stream_users = bx.ask(input_file, stream_size)
    bloom_filter(stream_users, ask)
f.close()
print("Duration : ", time.time() - start_time)
