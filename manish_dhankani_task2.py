from pyspark import SparkContext, SparkConf
from operator import add
from operator import itemgetter
import itertools
import csv
import time
import sys

baskets = {}

def read_input(sc):
    # start1 = time.time()
    # path = "C:\\Users\\Manish Dhankani\\Documents\\USC\\Spring 2019\\INF553 Data Mining\Assignment\\input.csv"
    path = sys.argv[3]
    data = sc.textFile(path)
    csv_data = data.mapPartitions(lambda x: csv.reader(x))
    head = csv_data.first()
    csv_data = csv_data.filter(lambda x: x != head)
    # print(csv_data.take(5))
    # end1 = time.time()
    # print("Time for read-input:", end1 - start1)
    return csv_data

def apriori(support_threshold, basket, totalsize):

    # start2 = time.time()
    size = 1
    # basket = [x for x in basket.toLocalIterator()]
    baskets = list(basket)
    reduced_threshold = support_threshold * (float(len(baskets)) / float(totalsize))
    # print("reduced threshold is", reduced_threshold)
    frequent_itemset = list()
    # candidatesPhase1 = list()
    # print("start of apriori")
    while True:

        if size == 1:
            frequent_items = set()
            itemCount = {}

            for basket in baskets:
                # print(item)
                # print(type(item))
                for item in basket:
                    if item in itemCount:
                        itemCount[item] += 1
                        if (itemCount.get(item) >= reduced_threshold):
                            frequent_items.add(item)
                    else:
                        itemCount[item] = 1

            # print("for singletons",frequent_items)

            if frequent_items:
                candidate_set = frequent_items
                # print(candidate_set)
                for i in frequent_items:
                    frequent_itemset.append(tuple([i]))

        elif size == 2:
            # candidate_set = list(set(itertools.chain.from_iterable(candidate_set)))
            # candidate_set = set(candidate_set)
            candidate_set = itertools.combinations(candidate_set, size)
            # print(candidate_set)
            pairCount = {}
            frequent_items.clear()
            for item in candidate_set:
                itemset = set(item)
                # print(item)
                # print(type(item))
                for basket in baskets:
                    if itemset.issubset(basket):
                        if item in pairCount:
                            pairCount[item] += 1
                            if(pairCount.get(item) >= reduced_threshold):
                                item = tuple(sorted(item))
                                frequent_items.add(item)
                        else:
                            pairCount[item] = 1

            if frequent_items:
                candidate_set = frequent_items
                # print(candidate_set)
                frequent_itemset.extend(frequent_items)
                # print(frequent_itemset)

        else:
            candidate_set = list(set(itertools.chain.from_iterable(candidate_set)))
            # print(candidate_set)
            candidate_set = itertools.combinations(candidate_set, size)
            frequent_items.clear()
            sizeItemCount = {}

            for item in candidate_set:
                itemset = set(item)
                # print(type(item))
                # print(basket1)
                for basket in baskets:
                    # print(type(basket))
                    if itemset.issubset(basket):
                        if item in sizeItemCount:
                            sizeItemCount[item] += 1
                            if(sizeItemCount.get(item) >= reduced_threshold):
                                item = tuple(sorted(item))
                                frequent_items.add(item)
                        else:
                            sizeItemCount[item] = 1


            if frequent_items:
                candidate_set = frequent_items
                # print(candidate_set)
                frequent_itemset.extend(frequent_items)

        size += 1
        # candidate_set = frequent_items
        if not frequent_items:
            # frequent_itemset = [item for sublist in frequent_itemset for item in sublist]
            return frequent_itemset


def finalCandidatesCount(baskets, phase1Candidates):
    finalItemCount = {}
    baskets = list(baskets)

    for candidate in phase1Candidates:
        # candidate = [candidate]
        # candidate = set(candidate)
        # print(candidate)
        candidateset = set(candidate)
        for basket in baskets:
            if candidateset.issubset(basket):
                if candidate in finalItemCount:
                    finalItemCount[candidate] += 1
                else:
                    finalItemCount[candidate] = 1

    return finalItemCount.items()


def task(csv_data, case, support_threshold, filter_threshold):
    # start3 = time.time()
    if case == 1:
        # print(csv_data.take(10))
        bucket_data = csv_data.map(lambda x: (x[0], x[1])).groupByKey().filter(lambda x: len(x[1]) > filter_threshold).mapValues(set)
        # print(type(bucket_data))
        # print(bucket_data.take(10))

    bucket_data = bucket_data.map(lambda x: x[1])
    # print(bucket_data.take(10))
    totalsize = bucket_data.count()
    # print(bucket_data.getNumPartitions())
    # print(totalsize)

    #1st Map Reduce Operation

    # map1Output = apriori(support_threshold, bucket_data.collect(), totalsize)
    # print(map1Output)
    map1Output = bucket_data.mapPartitions(lambda basket: apriori(support_threshold, basket, totalsize)).map(lambda x: (x, 1))

    # print(bucket_data.getNumPartitions())


    reduce1Output = map1Output.reduceByKey(lambda x,y: (1)).keys().collect()
    reduce1Output.sort(key=lambda x: [len(x), x])
    # end3 = time.time()
    # print("Time for 1st MR", end3 - start3)
    # print("Phase 1 output is")
    # print(reduce1Output)

    with open(sys.argv[4], 'w+', encoding='utf-8') as outfile:
    # with open('task2output2.txt', 'w+', encoding='utf-8') as outfile:
        outfile.write("Candidates:")
        outfile.write("\n")
        size = 1
        out = ""
        for item in reduce1Output:
            # print(item[0])
            if(len(item) == size):
                if(len(item) == 1):
                    out += "('" + item[0] + "'),"
                else:
                    out += str(item) + ","
            else:
                outfile.write(out[:-1] + "\n\n")
                size += 1
                out = ""
                out += str(item) + ","
        outfile.write(out[:-1])



    # 2nd Map Reduce Operation
    # start4 = time.time()
    # map2Output = finalCandidatesCount(bucket_data.collect(), reduce1Output)
    map2Output = bucket_data.mapPartitions(lambda basket: finalCandidatesCount(basket, reduce1Output))
    reduce2Output = map2Output.reduceByKey(lambda x, y: x+y)

    frequentItems = reduce2Output.filter(lambda x: x[1] >= support_threshold)

    frequentItems = frequentItems.keys().collect()
    frequentItems.sort(key=lambda x: [len(x), x])
    # end4 = time.time()
    # print("time for 2nd MR", end4 - start4)
    # print("****************************************************************")
    # print(frequentItems)
    with open(sys.argv[4], 'a', encoding='utf-8') as outfile:
    # with open('task2output2.txt', 'a', encoding='utf-8') as outfile:
        outfile.write("\n")
        outfile.write("Frequent Itemsets:")
        outfile.write("\n")
        size = 1
        out = ""
        for item in frequentItems:
            # print(item[0])
            if(len(item) == size):
                if(len(item) == 1):
                    out += "('" + item[0] + "'),"
                else:
                    out += str(item) + ","
            else:
                outfile.write(out[:-1] + "\n\n")
                size += 1
                out = ""
                out += str(item) + ","
        outfile.write(out[:-1])
        outfile.close()
    # print("Final output is:")
    # print(frequentItems)
    return frequentItems

from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    start = time.time()
    conf = SparkConf().setAppName("FrequentItemset").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    support_threshold = int(sys.argv[2])
    # support_threshold = 50
    filter_threshold = int(sys.argv[1])
    # filter_threshold = 70
    csv_data = read_input(sc)
    case = 1
    frequentItems = task(csv_data, case, support_threshold, filter_threshold)
    end = time.time()
    print("Durration:", end - start)