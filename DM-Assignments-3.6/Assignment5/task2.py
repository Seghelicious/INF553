from blackbox import BlackBox
import sys
import time
import binascii
import random
from statistics import mean, median


def create_hash_values(n):
    # a = random.sample(range(1, 1000), n)
    # b = random.sample(range(1, 1000), n)
    # p = random.sample(range(10000, 10000000), n)
    a = [332, 993, 568, 476, 380, 10, 991, 883, 517, 430, 552, 830, 805, 775, 726, 527]
    b = [572, 403, 428, 621, 786, 451, 790, 335, 970, 97, 88, 811, 71, 991, 601, 842]
    p = [6649385, 6475799, 4416863, 8564383, 5955983, 4433527, 380121, 1127229, 738500, 2007533, 6623519, 9440624,
         668655, 2632966, 1674740, 9491576]
    hash_values = []
    for i in range(n):
        hash_values.append([a[i], b[i], p[i]])
    return hash_values


def myhashs(s):
    result = []
    user_int = int(binascii.hexlify(s.encode('utf8')), 16)
    for h in hash_values:
        result.append(((h[0] * user_int + h[1]) % h[2]) % m)
    return result


def get_estimated_count(estimations):
    averages = []
    j = 0
    for i in range(4, num_hash_values):
        sub = estimations[j:i]
        averages.append(mean(sub))
        j = i
        i += 4
    return median(averages)


def get_trailing_zeroes(hash_value):
    binary = bin(hash_value)[2:]
    return len(binary) - len(binary.rstrip("0"))


def flajolet_martin(stream_users, ask):
    global actual_total, estimated_total
    estimations = []
    all_hash_values = []
    for user_id in stream_users:
        hash_values = myhashs(user_id)
        all_hash_values.append(hash_values)
    for i in range(num_hash_values):
        max_traling_zeroes = -1
        for hash_values in all_hash_values:
            trailing_zeros = get_trailing_zeroes(hash_values[i])
            if trailing_zeros > max_traling_zeroes:
                max_traling_zeroes = trailing_zeros
        estimations.append(2 ** max_traling_zeroes)
    estimated_count = round(get_estimated_count(estimations))
    estimated_total += estimated_count
    actual_total += len(set(stream_users))
    f.write(str(ask) + "," + str(len(set(stream_users))) + "," + str(estimated_count) + "\n")


# time python task2.py $ASNLIB/publicdata/users.txt 500 30 task2.csv
start_time = time.time()

# input_file = 'dataset/users.txt'
# stream_size = 300
# num_of_asks = 30
# output_file = 'output/task2.csv'

actual_total = 0
estimated_total = 0

input_file = sys.argv[1]
stream_size = int(sys.argv[2])
num_of_asks = int(sys.argv[3])
output_file = sys.argv[4]

num_hash_values = 16
m = 2 ** num_hash_values
hash_values = create_hash_values(num_hash_values)

f = open(output_file, "w")
f.write("Time,Ground Truth,Estimation\n")

bx = BlackBox()
for ask in range(num_of_asks):
    stream_users = bx.ask(input_file, stream_size)
    flajolet_martin(stream_users, ask)
f.close()
print(estimated_total / actual_total)
print("Duration : ", time.time() - start_time)
