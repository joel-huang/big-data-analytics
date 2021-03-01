import os
import sys
import string
import pyspark
import itertools
import numpy as np

conf = pyspark.SparkConf()
sc = pyspark.SparkContext(conf=conf)

datafiles_folder = sys.argv[1]
stopwords_file_path = sys.argv[2]
query_file_path = sys.argv[3]
out_file_path = sys.argv[4]

stopwords = [w.strip("\n") for w in open(stopwords_file_path, "r").readlines()]

def remove_stopwords(l):
    return " ".join([x for x in l.split(" ") if x not in stopwords])

def strip_punctuation(l):
    return l.translate(str.maketrans(dict.fromkeys(string.punctuation)))

def remove_stray_spaces(l):
    return " ".join(l.split())

def is_indep_number(s):
    if s.isdigit():
        return True
    try:
        float(s)
        return True
    except:
        return False

def remove_indep_numbers(l):
    return " ".join([x for x in l.split(" ") if not is_indep_number(x)])

def filter_empty_and_none(l):
    return l is not None and len(l) > 0

# Step 1 ======================================================================
# Preprocessing
# 1. To lowercase
# 2. Remove stopwords, drop punctuation, drop independent numbers
# =============================================================================
directory = ",".join(
    sorted([os.path.join(datafiles_folder, x)
        for x in os.listdir(datafiles_folder)
        if x != ".DS_Store"]
    )
)

datafile_rdd = sc.textFile(directory)

d = (
    datafile_rdd
    .map(lambda x: x.lower())
    .map(remove_stopwords)
    .map(strip_punctuation)
    .map(remove_indep_numbers)
    .map(remove_stray_spaces)
    .filter(filter_empty_and_none)
)

# Step 2 ======================================================================
# Compute term frequency (TF) of every word in a document.
# Compute document frequency (DF) of every word.
# =============================================================================
def compute_tf(partition):
    # partition is Iterator<(word, 1)>
    count_dict = dict()
    for element in partition:
        word = element[0]
        if word in count_dict:
            count_dict[word] += 1
        else:
            count_dict[word] = 1
    for (word, count) in [(k, count_dict[k]) for k in count_dict]:
        yield (word, count)

# Lines is an RDD containing all the words
# in all the documents, partitioned by document.
lines = (
    d.flatMap(lambda x: x.strip("\n").split(" "))
)

tf = (
    lines.map(lambda x: (x, 1))
    .mapPartitions(compute_tf)
)

# DF is the number of documents containing the term:
# Within each partition, remove duplicates, reduce by key.
def unique_pairs_within_partition(partition):
    # Taking the set of the partition is
    # the same as taking the unique keys
    count_dict = dict()
    for element in partition:
        if element not in count_dict:
            count_dict[element] = 1
            yield (element, 1)

df = dict(
    lines.mapPartitions(unique_pairs_within_partition)
    .reduceByKey(lambda x, y: x + y)
    .collect()
)

print(f"DF: {df}")

# Step 3 ======================================================================
# Compute TF-IDF of every word w.r.t a document.
# You can use key-value pair RDD and the groupByKey() API. Use log base 10
# for TF-IDF calculation.
# =============================================================================
N = lines.getNumPartitions()

def tfidf_partition(partition):
    partition = list(partition)
    tfidf_dict = dict.fromkeys(df.keys(), 0)
    for element in partition:
        word, tf = element
        tfidf_dict[word] = (1 + np.log10(tf)) * np.log10(N / df[word])
    for word, tfidf in tfidf_dict.items():
        yield (word, tfidf)

tfidf = (
    tf.mapPartitions(tfidf_partition)
)


# Step 4 ======================================================================
# Compute normalized TF-IDF of every word w.r.t. a document.
# If the TF-IDF value of word1 is t1 and the sum of squares of the TF-IDF
# of all the words is s, then the normalized TF-IDF value of word1 is t1/sqrt(s).
# =============================================================================
def tfidf_normalized_partition(partition):
    partition = list(partition)
    S = sum(tfidf**2 for _, tfidf in partition)
    for element in partition:
        word, tfidf = element
        yield (word, tfidf / np.sqrt(S))

# Partition order is preserved in grouped list
# {"word": [0, 0, 0, 0.03, 0, ...]}
tfidf_normalized = dict(
    tfidf.mapPartitions(tfidf_normalized_partition)
    .groupByKey()
    .mapValues(list)
    .collect()
)

print(tfidf_normalized)

# Assume the matrix can fit in the reducer node memory
word_order = list(tfidf_normalized.keys())
normalized_tfidf_matrix = np.array(list(tfidf_normalized.values()))

# Step 5/6 ====================================================================
# Compute the relevance of each document w.r.t a query.
# Sort and get top-k documents.
# One line per review in the following format: <docID> <relevance score>
# The output should be sorted in descending order of the relevance of the
# documents to the query. Use k = 5.
# =============================================================================
k = 5

# Treat the query as another document
# Don't assume that it can fit in main memory
query_rdd = sc.textFile(query_file_path)

query_list = (
    query_rdd.flatMap(lambda x: x.strip("\n").split(" "))
    .map(lambda x: (x, 1))
    .reduceByKey(lambda x, y: x)
    .collect()
)

query_dict = dict.fromkeys(word_order, 0)
for (query, _) in query_list:
    if query not in query_dict:
        raise Exception(f"Query {query} not found in the set of words in all documents.")
    query_dict[query] = 1
query_vector = np.array(list(query_dict.values()))

# Compute similarity of each column of M with the q vector.
def cosine_similarity(q, M):
    assert (q.ndim < 2 and M.ndim > 1)
    print(f"Computing cosine similarity between q: {q.shape} and M: {M.shape}.")
    return np.dot(q, M) / (np.linalg.norm(q) * np.linalg.norm(M))

similarity = cosine_similarity(query_vector, normalized_tfidf_matrix)
relevances = sorted([(doc_id, relevance) for doc_id, relevance in enumerate(similarity)],
                key=lambda x: x[1],
                reverse=True)

print(relevances)

with open(out_file_path, 'w+') as f:
    for (doc_id, relevance) in relevances[:k]:
        f.write(f"{doc_id} {relevance}\n")

sc.stop()