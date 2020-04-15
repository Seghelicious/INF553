from pyspark import SparkConf, SparkContext
from blackbox import BlackBox
import time
import binascii
import sys

conf = SparkConf()
sc = SparkContext()
sc.setLogLevel("ERROR")
start = time.time()

input_file = 'dataset/users.txt'
stream_size = 100
num_of_asks = 30
output_file = 'output/hru.csv'

bx = BlackBox()

true_negative = 0
false_positive = 0
ground_truth_set = set()
bits = [0] * 69997
a = [3, 73]
b = [79, 803]
p = [1543, 6151]

with open(output_file, 'w+') as file:
    file.write("Time,FPR\n")


def myhashs(user_id):
    user_int = int(binascii.hexlify(user_id.encode('utf8')), 16)
    result = []
    for i in range(len(a)):
        hashed_value = ((user_int * a[i] + b[i]) % p[i]) % 69997
        result.append(hashed_value)
    return result


def bloomFilter(counter, users):
    global true_negative, false_positive
    for user in users:
        flag = False
        hashes = myhashs(user)
        # print(hashes)
        for i in range(len(hashes)):
            if bits[hashes[i]] == 0:
                flag = True
                bits[hashes[i]] = 1
        if flag:
            true_negative += 1
        else:
            if user not in ground_truth_set:
                # print("############# Hello")
                false_positive += 1
        ground_truth_set.add(user)

    if true_negative != 0 and false_positive != 0:
        fpr = false_positive / float(true_negative + false_positive)
    else:
        fpr = 0
    # print(str(false_positive) + " #### " + str(true_negative) + " ##### " + str(fpr))
    with open(output_file, 'a') as file:
        file.write(str(counter) + "," + str(fpr) + "\n")


for counter in range(int(num_of_asks)):
    stream_users = bx.ask(input_file, int(stream_size))
    bloomFilter(counter, stream_users)

end = time.time()
print("Duration: ", end - start)
