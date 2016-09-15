import math
import random

import pandas as pd

'''##################################       Loading Data      ########################################'''
attributes = pd.read_csv('ecoli.csv', usecols=[0, 1, 2, 3, 4, 5, 6])
# print attributes
attribute_name_list = list(attributes.columns.values)
# print attribute_name_list
class_val = pd.read_csv('ecoli.csv', usecols=[7])
# print class_val
class_name_list = list(class_val.columns.values)
# print class_name_list
'''##################################      Normalizing Data       ########################################'''
parts = []


def normalization():
    for i in attribute_name_list:
        print i
        max_attribute_val = attributes[i].max()
        print "max_attribute_val is : ", max_attribute_val
        min_attribute_val = attributes[i].min()
        print "min is : ", min_attribute_val
        for index, row in attributes.iterrows():
            # print "row val ",row[i]
            if min_attribute_val < 0 or max_attribute_val > 1:
                # if row[i] < 0 or row[i] > 1:
                # print "inside if loop"
                # print "row val before normalization ", row[i]
                normalized_value = round((row[i] - min_attribute_val) / (max_attribute_val - min_attribute_val), 2)
                # print "normalized value is ",normalized_value
                row[i] = normalized_value
                # print "row val after normalization ", row[i]
                # print attributes
                # print attribute_name_list
                # getch = raw_input("getch")
    print attributes
    # attributes.to_csv("norm.csv")


'''##################################     Patitioning Data   ########################################'''


def partition():
    n = raw_input("Enter the number of partitions")
    n = int(n)
    shape = attributes.shape
    no_of_rows = shape[0]
    # print no_of_rows
    row_per_part = float(no_of_rows) / float(n)
    row_per_part = math.ceil(row_per_part)
    print "rows per partition is ", row_per_part
    index_list = range(0, no_of_rows)
    random.shuffle(index_list)
    # print index_list
    attributes['part'] = int(0)
    class_val['part1'] = int(0)
    for i in range(1, n + 1):
        # print "printing value of i = ", i
        count = 1
        while count <= row_per_part:
            if len(index_list) != 0:
                # print "Index list before choice \n", index_list
                num = random.choice(index_list)
                # print "selected num is : ", num
                index_list.remove(num)
                # print "Index list after  choice \n", index_list
                attributes.iloc[num, 7] = i
                class_val.iloc[num, 1] = i
                # print attributes.iloc[num, 7]
                # getch = raw_input("getch")
            count += 1
    # print attributes
    # print class_val
    # print class_val.groupby('part1').count()
    merged_data = pd.concat([attributes, class_val], axis=1)
    merged_data.drop('part1', axis=1, inplace=True)
    print merged_data
    part_list = merged_data.part.unique()
    # match_results.stadium.unique()
    part_list = sorted(part_list)
    for item in part_list:
        temp_df = merged_data[merged_data['part'] == int(item)]
        parts.append(temp_df)
    print parts


def return_partition():
    merged_data = pd.concat([attributes, class_val], axis=1)
    merged_data.drop('part1', axis=1, inplace=True)
    no_of_partitions = raw_input("Enter the partitions to fetch")
    temp_df = merged_data[merged_data['part'] == int(no_of_partitions)]
    print temp_df


def train():
    print "train"


normalization()
partition()
return_partition()
train()
