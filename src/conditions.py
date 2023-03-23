# -*- coding: utf-8 -*-
# @Author: Eduardo Santos
# @Date:   2023-03-12 22:26:26
# @Last Modified by:   Eduardo Santos
# @Last Modified time: 2023-03-23 22:30:02

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf
from datetime import datetime
from time import time

def build_pairs(basket, filtered_diseases):
    """
    Returns a list of bigrams that only contain diseases from the filtered list.
    
    Parameters:
    - basket: a tuple representing a basket of items
    - filtered_diseases: a list of diseases above the support treshold to include in the bigrams
    
    Returns:
    - a list of bigrams, where each bigram is a tuple of two diseases
    """
    basket_diseases = basket[1] # list of diseases in the current basket
    bigrams = []

    for i, disease_1 in enumerate(basket_diseases):
        # if the current disease is not in the list of filtered diseases, skip
        if disease_1 not in filtered_diseases:
            continue

        for disease_2 in basket_diseases[i+1:]:
        # if the current disease is not in the list of filtered diseases, skip
            if disease_2 not in filtered_diseases:
                continue

            # create a bigram from the two diseases and append it to the list of bigrams
            bigram = f"{disease_1},{disease_2}"
            bigrams.append((bigram, 1))
    
    return bigrams

def build_trios(basket, filtered_diseases, filtered_diseases2):
    """
    Returns a list of trigrams that contain diseases from the filtered list.
    
    Parameters:
    - basket: a tuple representing a basket of items
    - filtered_diseases: a list of diseases above the support treshold to include in the trigrams
    - filtered_diseases2: a list of bigrams to include in the trigrams
    
    Returns:
    - a list of trigrams, where each trigram is a tuple of three diseases
    """
    diseases = basket[1] # list of diseases in the current basket
    trigrams = []
    
    for i, desease1 in enumerate(diseases):
        if desease1 not in filtered_diseases:
            continue
        for j, desease2 in enumerate(diseases[i+1:], i+1):
            # if the current disease is not in the list of filtered diseases, skip
            if desease2 not in filtered_diseases:
                continue

            # create a bigram from the two diseases and check if it is in the 
            # list of filtered bigrams
            if f"{desease1},{desease2}" not in filtered_diseases2:
                continue

            for desease3 in diseases[j+1:]:
                # if the current disease is not in the list of filtered diseases, 
                # skip to the next iteration
                if desease3 not in filtered_diseases:
                    continue
                
                # check if the two bigrams that can be created from the trigram 
                # are in the list of filtered bigrams
                if f"{desease1},{desease3}" not in filtered_diseases2 \
                    or f"{desease2},{desease3}" not in filtered_diseases2:
                    continue

                # create a trigram from the three diseases and append it to the 
                # list of trigrams
                trigrams.append((f"{desease1},{desease2},{desease3}", 1))
    return trigrams

def get_prob_value(diseases, baskets, type):
    if type == "bigram":
        disease = diseases.split(",")[1]
    if type == "trigram":
        disease = diseases.split(",")[2]
    
    return len(baskets.filter(lambda x: disease in x[1]).collect())/baskets.count()

def get_support_value(diseases, diseases_support, type):
    if type == "bigram":
        disease = diseases.find(",")
    if type == "trigram":
        disease = diseases.rfind(",")
    
    key = diseases[:disease]

    return diseases_support.filter(lambda x: x[0] == key).collect()

def get_std_lift(support_A, prob_B, lift, total_baskets):
    max_value = max(support_A + prob_B - 1, 1 / total_baskets)
    constant = (support_A * prob_B)

    numerator = lift - (max_value / constant)
    denominator = (1 / constant) - (max_value / constant)

    return numerator / denominator

if __name__ == "__main__":

    if len(sys.argv) != 3:
        exit(-1)

    K = int(sys.argv[1])
    SUPPORT = 1000

    path = sys.argv[2]

    conf = SparkConf()
    conf.getAll()

    sc = SparkContext(appName="test")
        
    spark = SparkSession(sc)
    sc.setLogLevel("ERROR")

    textfile = sc.textFile(path)

    sc.addFile(path)

    before_baskets =  textfile.map(lambda line: line.split(",")) \
                                .map(lambda pair: (pair[2],[pair[4]])) \
                                    .reduceByKey(lambda a,b: a + b)

    baskets = before_baskets.mapValues(lambda x: sorted(set(x)))

    first_support = baskets.flatMap(lambda basket: 
                                    [(disease, 1) for disease in basket[1]]
                                    ).reduceByKey(lambda a,b: a + b) \
                                        .filter(lambda line: line[1] > SUPPORT)
    
    ##########################################################################
    #                                                                        #
    #                                BIGRAMS                                 #
    #                                                                        #
    ##########################################################################
    
    filtered_diseases = first_support.map(lambda line: line[0]).collect()

    bigrams_support = baskets.flatMap(lambda basket: 
                                    build_pairs(
                                        basket, 
                                        filtered_diseases)
                                    ).reduceByKey(lambda a,b: a + b) \
                                        .filter(lambda line: line[1] > SUPPORT)

    if K == 2:

        #all_bigrams = bigrams_support.sortBy(lambda line: line[1], False).collect()

        # top 10 bigrams
        top10_bigrams = bigrams_support.sortBy(lambda line: line[1], False).take(10)

        print("\nTop 10 bigrams:\n")
        print(top10_bigrams)

        rules = {key:[value] for key, value in bigrams_support.collect()}
        list_rules = []

        # get association rules (X) -> (Y)
        for key in rules:
            # get support of X
            support_x = get_support_value(key, first_support, "bigram")[0][1]

            # confidence: support(X U Y)/support(X)
            confidence = rules[key][0] /  support_x
            rules[key][0] = confidence

            # get probability of Y
            prob_y = get_prob_value(key, baskets, "bigram")

            # interest: confidence - prob(Y)
            interest = confidence - prob_y
            rules[key].append(interest)

            # lift: confidence / prob(Y)
            lift = confidence / prob_y
            rules[key].append(lift)

            # standard lift
            std_lift = get_std_lift(support_x, prob_y, lift, baskets.count())
            rules[key].append(std_lift)

            list_rules.append([key] + rules[key])

        association_rules = sc.parallelize(list_rules)
        association_rules = association_rules.filter(lambda line: line[4] > 0.2).sortBy(lambda line: line[4])

        # save the results
        with open("{0}/Top_10_Bigrams.csv".format("../results/bigrams"), "w+") as file:
            file.write("\n".join([b[0] for b in top10_bigrams]))
            
        format_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        association_rules.saveAsTextFile("{0}/Association_Rules {1}".format("../results/bigrams", format_time))

        printable5 = association_rules.collect()

        print("\nAssociation rules:\n")
        print(printable5)
    
    ##########################################################################
    #                                                                        #
    #                                TRIGRAMS                                #
    #                                                                        #
    ##########################################################################

    if K == 3:

        filtered_diseases2 = bigrams_support.map(lambda line: line[0]).collect()

        trigrams_support = baskets.flatMap(lambda basket: 
                                        build_trios(
                                                basket, 
                                                filtered_diseases, 
                                                filtered_diseases2)
                                                ).reduceByKey(lambda a,b: a + b) \
                                                    .filter(lambda line: line[1] > SUPPORT)

        #all_trigrams = trigrams_support.sortBy(lambda line: line[1], False).collect()

        top10_trigrams = trigrams_support.sortBy(lambda line: line[1], False).take(10)

        print("\nTop 10 trigrams:\n")
        print(top10_trigrams)

        association_rules = {key:[value] for key, value in trigrams_support.collect()}
        rules_list = []

        # get association rules (X,Y) -> (Z)
        for key in association_rules:
            # get support of X U Y
            support_xy = get_support_value(key, bigrams_support, "trigram")[0][1]

            # confidence: support(X U Y U Z)/support(X U Y)
            confidence = association_rules[key][0] /  support_xy
            association_rules[key][0] = confidence

            # get probability of Z
            prob_z = get_prob_value(key, baskets, "trigram")

            # interest: confidence - prob(Z)
            interest = confidence - prob_z
            association_rules[key].append(interest)

            # lift: confidence / prob(Z)
            lift = confidence / prob_z
            association_rules[key].append(lift)

            # standard lift
            std_lift = get_std_lift(support_xy, prob_z, lift, baskets.count())
            association_rules[key].append(std_lift)

            rules_list.append([key] + association_rules[key])

        association_rules = sc.parallelize(rules_list)
        association_rules = association_rules.filter(lambda line: line[4] > 0.2).sortBy(lambda line: line[4])

        # save the results
        with open("{0}/Top_10_Trigrams.csv".format("../results/trigrams"), "w+") as file:
            file.write("\n".join([t[0] for t in top10_trigrams]))

        format_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        association_rules.saveAsTextFile("{0}/Association_Rules {1}".format("../results/trigrams", format_time))

        print("\nAssociation rules:\n")
        printable6 = association_rules.collect()

        print(printable6)
