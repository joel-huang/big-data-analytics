import os
import sys
import string
import pyspark
import itertools

conf = pyspark.SparkConf()
sc = pyspark.SparkContext(conf=conf)

datafiles_folder = sys.argv[1]
stopwords_file_path = sys.argv[2]
out_file_path = sys.argv[3]

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
datafile_rdd = sc.textFile(os.path.join(datafiles_folder))
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
# Compute the count of every word pair in the resulting documents. Note that
# <w1, w2> and <w2, w1> are considered the same word pair.
# =============================================================================
pairs = (
    d.flatMap(lambda x: itertools.combinations(x.strip("\n").split(" "), 2))
    .filter(lambda x: x[0] != x[1])
    .map(lambda x: (x, 1))
    .reduceByKey(lambda x, y: x + y)
)

# Step 3 ======================================================================
# Sort the list of word pairs in descending order and obtain the top-k
# frequently occurring word pairs. Use k=5.
# =============================================================================
k = 5
ranks = sorted(pairs.collect(), key=lambda x: x[1], reverse=True)

# Step 4 ======================================================================
# Output one line per word pair: <word pair> <count> sorted in descending order
# =============================================================================

with open(out_file_path, 'w+') as f:
    for ((w1, w2), count) in ranks[:k]:
        f.write(f"{w1} {w2} {count}\n")

sc.stop()