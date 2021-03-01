# Lab 2
This lab has two tasks and requires you to work with a large set of documents and search for relevant documents. You will need to understand the concepts of RDD, transformation and action in Apache Spark, and implement an algorithm at the RDD level. To run the Spark programs:
```
spark-submit lab2_task1.py data/datafiles data/stopwords.txt out/out_task1.txt
spark-submit lab2_task2.py data/datafiles data/stopwords.txt data/query.txt out/out_task2.txt
```

## Configuration
Spark 3.0.1, Scala 2.12.10, OpenJDK 64-Bit Server VM 11.0.9, Python 3, Runs on MacOS Mojave 10.14.6

## Data
35 text files with various content.
```
head -n 1 f11.txt
```
```
I truly love the humor of South Park. It's social/political commentary and satirical wit and bathroom absurdity rules. No holds barred! I live to breathe another day in anticipation of Trey Parker's next installment, whether it be South Park or some other venture. South Park is truly the work of a genius. Thank you, Sir Parker. (In Britian they often \"knight\" certian individuals (usually after they are \"old\" or \"dead\") for their contributions to their society) :)Sometimes it is dry. Sometimes it is wet. And sometimes it is too moist for humor. But somebody has to do it!I would like to see new South Park episodes until the day I leave this mortal existence. However, I know that Sir Parker would want to \"move on\". However, I would much like to see South Park from the eyes of a 90 year old Trey Parker. What wisdom would we see then?%      
```

## Task 1: Frequency of word pairs
Write a Spark program to find the top-k frequently occurring word pairs in a set of documents.

**Input**
- Set of documents (in `datafiles` folder)
- Stopwords to remove (in `stopwords.txt`).

**Output**
- One line per word pair in the following format: `<word pair> <count>`. You should sort the word pairs in descending order of frequency of occurrence. Use k = 5.

**Algorithm**
1. Preprocess the documents (to lowercase, remove stopwords, drop punctuation, drop independent numbers).
2. Compute the count of every word pair in the resulting documents. Note that `<word1, word2>` and `<word2, word1>` are considered as the same word pair.
3. Sort the list of word pairs in descending order and obtain the top-k frequently occurring word pairs.

## Task 2: TF-IDF
A document can be modelled as a vector of words (or terms). Each entry in the vector is a TF-IDF value that reflects how important a word is to a document in a collection, computed as
```
TFIDF = (1 + log(TF)) * log (N/DF)
```
where `N` is total number of documents, `TF` is the count of the word in a document, and `DF` is the count of documents having the word.

A query can also be represented as a vector where each entry represents a word with a value 1 if the word is in the query, and 0 otherwise. We can compute a relevance score for each document `d` to a query `q` based on the based on the cosine similarity of their corresponding vectors `V1` and `V2` and rank the documents with respect to a query:
```
relevance(q,d) = cos(V1, V2) = (V1 dot V2) / (|V1| * |V2|)
```

**Input**
- Set of documents (in `datafiles` folder)
- Set of keywords for a query (in `query.txt`)
- Stopwords to remove (in `stopwords.txt`).


**Output**
One line per review in the following format: `<docID> <relevance score>`. The output should be sorted in descending order of the relevance of the documents to the query. Use k = 5.

**Algorithm**
1. Preprocess the documents (to lowercase, remove stopwords, drop punctuation, drop independent numbers).
2. Compute term frequency (TF) of every word in a document.
3. Compute TF-IDF of every word w.r.t a document.
You can use key-value pair RDD and the `groupByKey()` API. Use log base 10 for TF-IDF calculation.
4. Compute normalized TF-IDF of every word w.r.t. a document. If the TF-IDF value of word1 in doc1 is `t1` and the sum of squares of the TF-IDF of all the words in doc1 is S, then the normalized TF-IDF value of word1 is `t1/sqrt(S)`
5. Compute the relevance of each document w.r.t a query.
6. Sort and get top-k documents.

## Notes
**Resilient Distributed Dataset (RDD)** A collection of elements partitioned across the nodes of the cluster that can be operated on in parallel. RDDs are created starting with a file in the HDFS, or an existing Scala collection in the driver program, and transforming it. Users may also ask Spark to persist an RDD in memory, allowing it to be reused efficiently across parallel operations. Finally, RDDs automatically recover from node failures.

**Using RDD** https://spark.apache.org/docs/latest/rdd-programming-guide.html#working-with-key-value-pairs