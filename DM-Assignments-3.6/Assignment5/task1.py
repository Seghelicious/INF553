from blackbox import BlackBox
import binascii
import random
import sys
import time


def create_hash_values(n):
    # a = random.sample(range(1, 1000), n)
    # b = random.sample(range(1, 1000), n)
    # p = random.sample(range(10000, 10000000), n)
    # a = [332, 993, 568, 476, 380, 10, 991, 883, 517, 430, 552, 830, 805, 775, 726, 527]
    # b = [572, 403, 428, 621, 786, 451, 790, 335, 970, 97, 88, 811, 71, 991, 601, 842]
    # p = [6649385, 6475799, 4416863, 8564383, 5955983, 4433527, 380121, 1127229, 738500, 2007533, 6623519, 9440624,
    #      668655, 2632966, 1674740, 9491576]
    a = [1, 5, 12, 66, 89]
    b = [2, 8, 10, 34, 23]
    p = [12917, 33863, 72671, 113623, 153359]
    hash_values = []
    for i in range(n):
        hash_values.append([a[i], b[i], p[i]])
    return hash_values


def myhashs(s):
    result = []
    user_int = int(binascii.hexlify(s.encode('utf8')), 16)
    hash_values = create_hash_values(5)

    for h in hash_values:
        # result.append(((h[0] * user_int + h[1]) % h[2]) % m)
        result.append((h[0] * user_int + h[1]) % 69997)
    return result


def bloom_filter(stream_users, ask):
    global global_user_set, filter_bit_array
    false_positives = 0
    true_negatives = 0
    for user_id in stream_users:
        num_of_1s = 0
        hash_values = myhashs(user_id)
        for hash_value in hash_values:
            if filter_bit_array[hash_value] == 1:
                num_of_1s += 1
            else:
                filter_bit_array[hash_value] = 1
        if user_id not in global_user_set:
            if num_of_1s == len(hash_values):
                false_positives += 1  # False positive: x not in S, but identified as in S
            else:
                true_negatives += 1  # True nagative: x not in S, and identified as not in S
        global_user_set.add(user_id)
    if false_positives == 0 and true_negatives == 0:
        fpr = 0.0
    else:
        fpr = float(false_positives / float(false_positives + true_negatives))
    f.write(str(ask) + "," + str(fpr) + "\n")


if __name__=="__main__":
    # time python3 task1.py $ASNLIB/publicdata/users.txt 500 30 task1.csv
    start_time = time.time()

    input_file = 'dataset/users.txt'
    stream_size = 100
    num_of_asks = 300
    output_file = 'output/task1.csv'

    # input_file = sys.argv[1]
    # stream_size = int(sys.argv[2])
    # num_of_asks = int(sys.argv[3])
    # output_file = sys.argv[4]

    filter_bit_array = [0] * 69997
    global_user_set = set()

    f = open(output_file, "w")
    f.write("Time,FPR\n")

    bx = BlackBox()
    for ask in range(num_of_asks):
        stream_users = bx.ask(input_file, stream_size)
        bloom_filter(stream_users, ask)
    f.close()
    print("Duration : ", time.time() - start_time)
