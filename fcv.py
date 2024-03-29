import math
import random

import pandas as pd

##################################       Loading Data      ########################################
attributes = pd.read_csv('ecoli.csv', usecols=[0, 1, 2, 3, 4, 5, 6])
attribute_name_list = list(attributes.columns.values)
class_val = pd.read_csv('ecoli.csv', usecols=[7])
class_name_list = list(class_val.columns.values)
total_list = attribute_name_list + class_name_list
print total_list


##################################      Normalizing Data       ########################################


def normalization():
    choice = raw_input("Do you want to normalize data")
    choice = choice.lower()
    if choice == 'y':
        for i in attribute_name_list:
            print i
            max_attribute_val = attributes[i].max()
            print "max_attribute_val is : ", max_attribute_val
            min_attribute_val = attributes[i].min()
            print "min is : ", min_attribute_val
            for index, row in attributes.iterrows():
                normalized_value = round((row[i] - min_attribute_val) / (max_attribute_val - min_attribute_val), 3)
                row[i] = normalized_value
        print attributes
    else:
        print "Proceeeding without Normalization"


##################################     Patitioning Data   ########################################


def partition():
    n = raw_input("Enter the number of partitions")
    n = int(n)
    shape = attributes.shape
    no_of_rows = shape[0]
    row_per_part = float(no_of_rows) / float(n)
    row_per_part = math.ceil(row_per_part)
    print "rows per partition is ", row_per_part
    index_list = range(0, no_of_rows)
    random.shuffle(index_list)
    attributes['part'] = int(0)
    class_val['part1'] = int(0)
    for i in range(1, n + 1):
        count = 1
        while count <= row_per_part:
            if len(index_list) != 0:
                num = random.choice(index_list)
                index_list.remove(num)
                attributes.iloc[num, 7] = i
                class_val.iloc[num, 1] = i
            count += 1
    merged_data = pd.concat([attributes, class_val], axis=1)
    merged_data.drop('part1', axis=1, inplace=True)
    print merged_data
    y = raw_input("merge")
    part_list = merged_data.part.unique()
    parts = []
    part_list = sorted(part_list)
    for item in part_list:
        temp_df = merged_data[merged_data['part'] == int(item)]
        parts.append(temp_df)
    print parts
    final_data = parts
    print type(final_data)
    print len(parts)
    print len(final_data)
    print "train"

    for item in final_data:
        test_data = []
        chosen_part = int(item.part.unique())
        test_data.append(item)
        print "traiing data\n"
        for j in final_data:
            if not (int(j.part.unique()) == chosen_part):
                print j
        print "TEST DATA \n" + "#" * 100
        print test_data


def return_partition():
    merged_data = pd.concat([attributes, class_val], axis=1)
    merged_data.drop('part1', axis=1, inplace=True)
    no_of_partitions = raw_input("Enter the partitions to fetch")
    temp_df = merged_data[merged_data['part'] == int(no_of_partitions)]
    print temp_df


if __name__ == '__main__':
    normalization()
    partition()
    return_partition()
