import sys
import pyspark

conf = pyspark.SparkConf()
sc = pyspark.SparkContext(conf=conf)
sqlContext = pyspark.SQLContext(sc)

review_file_path = sys.argv[1]
metadata_file_path = sys.argv[2]
out_file_path = sys.argv[3]

# Step 1 ======================================================================
# Find the number of unique reviewer IDs for each product from the review file.
# (Use pair RDD to accomplish this step, the key is the product ID/asin)
# One pair: e.g. ("5555991584", "A2EFCYXHNK06IS")
# =============================================================================
review_rdd = sqlContext.read.json(review_file_path).rdd
asin_id_rdd = review_rdd.map(lambda x: (x['asin'], x['reviewerID'])).distinct()
grouped_asin_id_rdd = asin_id_rdd.groupByKey()
asin_unique_id_rdd = grouped_asin_id_rdd.map(lambda pair: (pair[0], len(pair[1])))

# Step 2 ======================================================================
# Create an RDD, based on the metadata, consisting of key/value-array pairs,
# key is the product ID/asin and value should contain the price of the product.
# =============================================================================
metadata_rdd = sqlContext.read.json(metadata_file_path).rdd
asin_price_rdd = metadata_rdd.map(lambda x: (x['asin'], x['price'])) # Assume all asin already unique

# Step 3 ======================================================================
# Join the pair RDD in Step 2 with the set of product-ID and 
# unique reviewer ID count pairs calculated in Step 1.
# Result: e.g. ("5555991584", (1000, 9.49))
# =============================================================================
asin_unique_id_price_rdd = asin_unique_id_rdd.join(asin_price_rdd)

# Step 4 ======================================================================
# Display the product ID, unique reviewer ID count, and the product price
# for the top 10 products based on the unique reviewer ID count.
# =============================================================================
sorted_asin_unique_id_price_rdd = asin_unique_id_price_rdd.sortBy(lambda x: x[1][0], False)

with open(out_file_path, 'w+') as f:
    for (asin, (count, price)) in sorted_asin_unique_id_price_rdd.take(10):
        f.write(f"{asin} {count} {price}\n")

sc.stop()