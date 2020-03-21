import os
import sys
import xgboost
from pyspark import SparkContext
import time

os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3.6"
#export PYSPARK_PYTHON=python3.6

start_time = time.time()

# time /home/local/spark/latest/bin/spark-submit task2_1.py $ASNLIB/publicdata/yelp_train.csv $ASNLIB/publicdata/yelp_val.csv task2-output.csv
# train_folder_path = sys.argv[1]
# input_file_test = sys.argv[2]
# output_file = sys.argv[3]
input_file_train = 'dataset/'
input_file_test = 'dataset/yelp_val.csv'
output_file = 'output/task2.csv'
