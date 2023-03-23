
import sys
from pyspark import SparkContext
import re
import random
import time


##########################################################################
#                                                                        #
#                                EX 2.1                                  #
#                                                                        #
##########################################################################

# gets document and returns k-shingles
def shingles(document, k=9):

    #initial set
    shingles_set = set()

    # getting rid of punctuation, etc
    document[1] = re.sub(r'[^\w\s]', '', document[1])

    # split to chars
    chars_list = re.split('', document[1].lower())

    # create shingles with length k
    for i in range(len(chars_list) - k):
        chars = chars_list[i : i + k]
        shingle = ''.join(chars)
        shingles_set.add(shingle)

    # returns: doc_id, set_shingles
    return document[0], shingles_set

# hash a value
def hash_function(x, a, b, N): 
    return (((a * hash(x) + b) % PRIME_NUMBER ) % N)

# get signature matrix from min hashing
def min_hash(document, total_size=910000):
    
    # shingles
    x = document[1]

    # initial matrix
    signature_matrix = []

    # iterate through the defined number of hash functions (hi)
    for i in range(0, K_VALUE):

        # start value is infinite
        minhash = float('inf')

        # get random integers
        a,b = RANDOMS[i]

        N = total_size
        # for each shingle
        for value in x:
            # hash shingle
            h = hash_function(value, a, b, N)
            # if lower, replace with current value
            if h < minhash:
                minhash = h

        # append the lowest number
        signature_matrix.append(minhash)
    
    # returns: doc_id, signature matrix
    return document[0], signature_matrix

# gets a band and hashes it to a bucket
def hash_lsh(band): 

    # intial array
    hashes = []

    # for each row within the band
    for r in band:
        # hash
        hashes.append((r * len(band)) % PRIME_NUMBER)

    #returns: min value
    return min(hashes)

# gets signature and returns bucket values for each band
def get_bucket_values(signature):

    # list of bucket values
    bucket_values = []

    # for the entire signature, iterate over each band with r rows and hash it
    for idx in range(0, B_VALUE):
        
        
        start_of_band = idx * R_VALUE

        # if there is no more bands to hash
        if start_of_band > len(signature): 
            break 

        # get end of band
        end_of_band = min(start_of_band + R_VALUE, len(signature))

        # hash the band to a bucket
        bucket = hash_lsh(signature[start_of_band : end_of_band])

        # append bucket value
        bucket_values.append(bucket)

    # returns: doc_id, bucket values
    return bucket_values

# given the signatures, returns candidate pairs
def lsh_algorithm(signatures_matrix):

    # dict with buckets for each document
    k_buckets = {}

    # initial candidates list
    candidates = []

    # iterate over the signatures
    for doc in signatures_matrix:

        # get the bucket values for each signature
        bucket = get_bucket_values(signatures_matrix[doc])

        # iterate over the other signatures bucket values
        for b_doc in k_buckets:

            # iterate over the bucket values
            for i in range(len(bucket)):

                # if at least 1 bucket value is the same, then at least 1 band hashes to the same bucket -> candidate 
                if k_buckets[b_doc][i] == bucket[i]:

                    # because it is candidate, compare with jaccard similarity and append to candidates list
                    jacc_sim = jaccard_similarity(signatures_matrix[doc], signatures_matrix[b_doc])
                    candidates.append((doc, b_doc, jacc_sim))

                    # because we only need 1 hash value in the same bucket, no need to continue
                    break

        # add the bucket values for each signature
        k_buckets[doc] = bucket

    # returns: candidates
    return candidates

# calculate jaccard similarity
def jaccard_similarity(sig_matrix_1, sig_matrix_2):
    # get intersection of the 2 matrices
    intersection = len([sig_matrix_1[i] for i in range(0, len(sig_matrix_1)) if (sig_matrix_1[i] == sig_matrix_2[i])])
    # get union of the 2 matrices
    union = (len(sig_matrix_1) + len(sig_matrix_2)) - intersection
    # calculate jaccard similarity
    jaccard_sim = intersection / union

    return jaccard_sim


##########################################################################
#                                                                        #
#                                EX 2.2                                  #
#                                                                        #
##########################################################################



# jaccar similarity for shingles
def shingles_jaccard(shingles_1, shingles_2):
    # intersection
    intersection = len(list(set(shingles_1).intersection(shingles_2)))
    #union
    union = (len(set(shingles_1)) + len(set(shingles_2))) - intersection
    
    # jaccard
    return float(intersection) / union


# similarity of article with possible candidates
def article_shingles_similarity(doc_id, filtered_shingles):
    for x in filtered_shingles:
        if x[0] == str(doc_id):
            doc_shingles = x[1]

    for candidate in filtered_shingles:
        if str(doc_id) != candidate[0]:
            jacc = shingles_jaccard(doc_shingles, candidate[1])
            if jacc > 0.85:
                print(str(doc_id) + " jaccard sim with " + candidate[0] + ": " + str(jacc))


##########################################################################
#                                                                        #
#                                EX 2.3                                  #
#                                                                        #
##########################################################################


def false_positives(signatures_matrix, candidates):

    count = 0
    for pair in candidates:

        matrix_1 = signatures_matrix[pair[0]]
        matrix_2 = signatures_matrix[pair[1]]

        matrix_similarity = jaccard_similarity(matrix_1, matrix_2)

        if pair[2] > 0.85 and matrix_similarity < 0.85: 
            count += 1

    percentage = count/len(candidates)

    return percentage

def false_negatives(signatures_matrix, candidates):

    good_pairs = []
    docs = list(signatures_matrix)
    
    for i in range(len(docs)):
        for k in range(i + 1, len(docs)):
            
            matrix_1 = signatures_matrix[docs[i]]
            matrix_2 = signatures_matrix[docs[k]]
            matrix_similarity = jaccard_similarity(matrix_1, matrix_2)
            
            if matrix_similarity > 0.85:
                good_pairs.append((docs[i],docs[k]))

    tuple_pairs = [(x[0], x[1]) for x in candidates]

    false_negatives = len(set(good_pairs) - set(tuple_pairs))
    return false_negatives/len(candidates)





if __name__ == "__main__":

    path = str(sys.argv[1])
    R_VALUE = int(sys.argv[2])
    B_VALUE = int(sys.argv[3])

    #Example usage: spark-submit lsh.py ../data/covid_news_truncated.json 13 11

    sc = SparkContext(appName="lsh")
        
    sc.setLogLevel("WARN")

    textfile = sc.textFile(path)

    # Large prime value to use in hashing
    PRIME_NUMBER = 50000017

    # r and b values for > 90% with 85% similarity and < 5% for 60% similarity
    R_VALUE = 13
    B_VALUE = 11

    # number of hash functions
    K_VALUE = R_VALUE * B_VALUE

    # generate random array to use in hashing
    RANDOMS = [(random.randint(1,2**31 - 1), random.randint(1,2**31 - 1)) for i in range(0,K_VALUE)]


    print("\nEx 2.1: computing LSH.")

    final_shingles = textfile.map(lambda line: eval(line)) \
                .map(lambda dict: [dict["tweet_id"], dict["text"]]) \
                .map(shingles)


    final_minhash = final_shingles.map(min_hash)


    signatures_matrix = { document: sig_matrix for document, sig_matrix in final_minhash.collect() }

    candidates = lsh_algorithm(signatures_matrix)
    sorted_candidates = sc.parallelize(candidates).sortBy(lambda pair: - pair[2])

    final_results = sorted_candidates.collect()

    print("finished LSH algorithm. ")




    print("\nEx 2.2: example of similar articles to " + str(1349823098623819784) + ":")
    article = 1349823098623819784
    filtered_candidates = sorted_candidates.filter(lambda x: x[0] == str(article)).collect()

    if filtered_candidates:
        candidates_ids = [filtered_candidates[0][0]] + [x[1] for x in filtered_candidates]
        filtered_shingles = final_shingles.filter(lambda x: x[0] in candidates_ids).collect()

        article_shingles_similarity(article, filtered_shingles)

    
    print("\nEx 2.3: Computing FP and FN.")

    false_positives = false_positives(signatures_matrix, candidates)
    false_negatives = false_negatives(signatures_matrix, candidates)

    print("False positives: " + str(false_positives * 100) + " %")
    print("False negatives: " + str(false_negatives * 100) + " %")