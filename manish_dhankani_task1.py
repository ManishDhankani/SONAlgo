from pyspark import SparkContext, SparkConf
from operator import add
from operator import itemgetter
import itertools
import csv
import time
import sys

baskets = {}


def read_input(sc):
    path = sys.argv[3]
    # path = "C:\\Users\\Manish Dhankani\\Documents\\USC\\Spring 2019\\INF553 Data Mining\Assignment\\2\\small2.csv"
    data = sc.textFile(path)
    csv_data = data.mapPartitions(lambda x: csv.reader(x))
    head = csv_data.first()
    csv_data = csv_data.filter(lambda x: x != head)
    # print(csv_data.take(5))
    return csv_data


def apriori(support_threshold, basket, totalsize):
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
            # current_basket = itertools.combinations(basket, size)
            for basket in baskets:
                # print(type(item))
                for item in basket:
                    # print(type(item))
                    if item in itemCount:
                        itemCount[item] += 1
                        if (itemCount.get(item) >= reduced_threshold):
                            frequent_items.add(item)
                    else:
                        itemCount[item] = 1

            if frequent_items:
                candidate_set = frequent_items
                # print(candidate_set)
                for i in frequent_items:
                    frequent_itemset.append(tuple([i]))
                    # frequent_itemset.append(tuple([i]))


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
                    # print(type(basket))
                    if itemset.issubset(basket):
                        if item in pairCount:
                            pairCount[item] += 1
                            if(pairCount.get(item) >= reduced_threshold):
                                item = tuple(sorted(item))
                                frequent_items.add(item)
                        else:
                            pairCount[item] = 1

            # for pair, count in pairCount.items():
            #     if (count >= reduced_threshold):
            #         pair = tuple(sorted(pair))
            #         frequent_items.append(pair)

            if frequent_items:
                candidate_set = frequent_items
                # print(candidate_set)
                frequent_itemset.extend(frequent_items)

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
        # if (len(candidate) == 1):
        # candidate = [candidate]

        candidateset = set(candidate)
        for basket in baskets:
            if candidateset.issubset(basket):
                if candidate in finalItemCount:
                    finalItemCount[candidate] += 1
                else:
                    finalItemCount[candidate] = 1
            # print(basket)
            # if type(candidate) is str:
            #     if candidate in basket:
            #         if candidate in finalItemCount:
            #             finalItemCount[candidate] += 1
            #         else:
            #             finalItemCount[candidate] = 1
            # else:
            #     if set(candidate).issubset(basket):
            #         if candidate in finalItemCount:
            #             finalItemCount[candidate] += 1
            #         else:
            #             finalItemCount[candidate] = 1

    return finalItemCount.items()


def task(csv_data, case, support_threshold, filter_threshold):
    if case == 1:
        # print(csv_data.take(10))
        bucket_data = csv_data.map(lambda x: (x[0], x[1])).groupByKey().mapValues(set)
        # print(type(bucket_data))
        # print(bucket_data.take(10))
        # bucket_data = csv_data.map(lambda x: (x[0], x[1])).groupByKey().filter(lambda x: len(x[1]) > filter_threshold).mapValues(set)
    elif case == 2:
        bucket_data = csv_data.map(lambda x: (x[1], x[0])).groupByKey().mapValues(set)
    bucket_data = bucket_data.map(lambda x: x[1])
    # print(bucket_data.take(10))
    totalsize = bucket_data.count()
    # print(bucket_data.getNumPartitions())
    # print(totalsize)

    # 1st Map Reduce Operation

    # print(map1Output)
    map1Output = bucket_data.mapPartitions(lambda basket: apriori(support_threshold, basket, totalsize)).map(lambda x: (x, 1))

    # print(bucket_data.getNumPartitions())

    reduce1Output = map1Output.reduceByKey(lambda x, y: (1)).keys().collect()
    # print(reduce1Output)
    reduce1Output.sort(key=lambda x: [len(x), x])

    with open(sys.argv[4], 'w', encoding='utf-8') as outfile:
    # with open('task2wed1.txt', 'w+', encoding='utf-8') as outfile:
        outfile.write("Candidates:")
        outfile.write("\n")
        size = 1
        out = ""
        for item in reduce1Output:
            # print(item[0])
            if (len(item) == size):
                if (len(item) == 1):
                    out += "('" + item[0] + "'),"
                else:
                    out += str(item) + ","
            else:
                outfile.write(out[:-1] + "\n\n")
                size += 1
                out = ""
                out += str(item) + ","
        outfile.write(out[:-1] + "\n")

    # 2nd Map Reduce Operation
    map2Output = bucket_data.mapPartitions(lambda basket: finalCandidatesCount(basket, reduce1Output))
    reduce2Output = map2Output.reduceByKey((lambda x, y: x + y))

    frequentItems = reduce2Output.filter(lambda x: x[1] >= support_threshold)

    frequentItems = frequentItems.keys().collect()
    frequentItems.sort(key=lambda x: [len(x), x])
    # print("****************************************************************")
    # print(frequentItems)
    with open(sys.argv[4], 'a', encoding='utf-8') as outfile:
    # with open('task2wed1.txt', 'a', encoding='utf-8') as outfile:
        outfile.write("\n")
        outfile.write("Frequent Itemsets:")
        outfile.write("\n")
        size = 1

        out = ""
        for item in frequentItems:
            # print(item[0])
            if (len(item) == size):
                if (len(item) == 1):
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
    # support_threshold = 7
    filter_threshold = 70
    # case = 2
    case = int(sys.argv[1])
    csv_data = read_input(sc)
    frequentItems = task(csv_data, case, support_threshold, filter_threshold)
    end = time.time()
    print("Durration:", end - start)