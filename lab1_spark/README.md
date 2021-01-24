# Lab 1
Write a Spark program to find the top 10 products based on the number of user reviews. To run the Spark program:
```
spark-submit lab1.py data/reviews_Digital_Music.json data/meta_Digital_Music.json out/out.txt
```

## Configuration
Spark 3.0.1, Scala 2.12.10, OpenJDK 64-Bit Server VM 11.0.9, Python 3, Runs on MacOS Mojave 10.14.6

## Data
Use the Digital Music review file (reviews_Digital_Music.json) and metadata (meta_Digital_Music.json) from the Amazon product dataset (http://jmcauley.ucsd.edu/data/amazon/links.html). Download both files from the “Per- category files” section.

### Reviews json
```
head -n 1 reviews_Digital_Music.json
```

```
// Very first line of file, just formatted for easier reading
{
    "reviewerID": "A2EFCYXHNK06IS",
    "asin": "5555991584",
    "reviewerName": "Abigail Perkins \"Abby &#34;Reads Too Much&#34...", 
    "helpful": [4, 5],
    "reviewText": "The anthemic title track begins &quot;The Memory Of  Trees&quot;, her fourth CD release.  Wordless vocalizations and forceful percussion propel this song into the stratosphere.  &quot;Anywhere Is&quot;, the lead single from this CD, is typical of her earlier fare like &quot;Orinoco Flow&quot;, but contains a magic all its own.  The most endearing aspect of this song is its lyrics.  Roma Ryan, Enya's long time lyricist, deserves much credit for her amazing lyrics on this song.  Relax and fall into the images the music and the words make together.  &quot;Pax Deorum&quot; is like a hurricane to me.  It begins ominously, like the calm before the storm.  It builds into a frighteningly wicked storm of immense proportions with strings and drums and vocals everywhere. Then, in the middle, it stops, and small lull transpires while angels come down and sing to us.  Then, just as suddenly, the wind comes up again and we are back into the thick of things.  And then, the end comes as suddenly as the beginning.  &quot;Athair Ar Neamh&quot; is a beautiful ballad which is more lyrical than any other ballad Enya has produced thus far.  In this song, Enya sounds very similar to her sister, Maire Brennan, lead singer of the group Clannad.  &quot;From Where I Am&quot; is another solo piano piece, which embodies the title perfectly.  I can picture myself staring at the wide expanse of the ocean or the land from a high cliff.  &quot;China Roses&quot; is a song of love and a song of the past.  You can feel the slow moving but intensely powerful energy in the undercurrent of this song.  Beautiful synthesized sounds add to the atmosphere here.  &quot;Hope Is A Place&quot; sounds melancholy, like someone has given up hope, and Enya is trying to tell them that it's not too late, it's never too late.  For all in love, hope has a place in YOUR soul.  This is such a sweet ballad with a simplistic arrangement.  &quot;Tea-House Moon&quot; is another instrumental track which embodies its title perfectly.  The sparkling synthesizers and melodies bring to mind a late evening tea ceremony underneath a bright full moon on a late summer's eve.  &quot;Once You Had Gold&quot; is a soft waltz, a lament, if you will, of something had and lost, here and gone.  &quot;La Sonadora&quot; is the only Enya song to be written in Spanish, and includes some unusual but effective chord changes.  A song to relax by.  &quot;On My Way Home&quot; starts off with a slow organ introduction and moves into a complex bassline as Enya travels on her journey home.  It seems as if your mind walks while listening to this song, with a stop in the bridge as sweet flutes massage your tired mind.  A fabulous way to end a fabulous CD.",
    "overall": 5.0,
    "summary": "Enya Experiments And Succeeds",
    "unixReviewTime": 978480000,
    "reviewTime": "01 3, 2001"
}
```

### Metadata json
```
head -n 1 meta_Digital_Music.json
```

```
// Very first line of file, just formatted for easier reading
{
    'asin': '5555991584',
    'title': 'Memory of Trees',
    'price': 9.49,
    'imUrl': 'http://ecx.images-amazon.com/images/I/51b5WDjdhPL._SX300_.jpg',
    'related': {
            'also_bought': ['B000002LRT', 'B000002LRR', 'B000050XEI', 'B000002MSM', 'B000B8QEYC', 'B001GQ2TGA', 'B000008FEA', 'B002RV01QI', 'B000002NJH', 'B000024V8E', 'B000JVSUXY', 'B00005S8ME', 'B00005K8EC', 'B000J233U8', 'B000J233SK', 'B000JFF2WW', 'B000J233TE', 'B0060ANYZ2', 'B002ZDOXLW', 'B0043ZDU1E', 'B000002U6E', 'B003LN9DRE', 'B000000WFU', 'B00002MG3U', 'B000002URV', 'B00006LJ72', 'B006C4P7BU', 'B00005UF2F', 'B001G9LVGG', 'B000CNF4LU', 'B000CNF4L0', 'B0007GAEGC', 'B00003OP2L', 'B005RYF5H2', 'B0069BUX0G', 'B00006JIAN', 'B000000WC1', 'B000000WF7', 'B001662F64', 'B000060O30', 'B003Y35H44', 'B009CSVPLY', 'B00DD348M2', 'B00005J9UN', 'B000TSQCHS', 'B000J233TY', 'B000001GBJ', 'B00005UE4B', 'B00000IL1K', 'B000BI1YJC', 'B0007M22TI', 'B004BBDHEK', 'B000GRUS22', 'B00J3V97NS', 'B0000062I1', 'B00005QZWI', 'B000UZ4GXC', 'B000WSRPOO', 'B002UZXJA6', 'B0000248JR', 'B000002MHL', 'B0002RUAAQ', 'B00AJLHVB6', 'B004M8SQB6', 'B000ELJAW4', 'B00000I609', 'B000AXWHPI', 'B001932LMW', 'B0007GAEVC', 'B00417HV6E', 'B00020HEH0', 'B0002YCVQK', 'B00E1C4SJC', 'B005FYCF2M', 'B00004UDNP', 'B00DJYJWTO', 'B000002VUC', 'B006ZZANFG', 'B000003BR4', 'B0000CC85G', 'B000KRNCYY', 'B000005J7X', 'B001662F6Y', 'B00004OCQG', 'B0000C7PQK', 'B001C4E6DA', 'B001662F7S', 'B00005QZCS', 'B000000NGH', 'B00063F8BC', 'B002HMHXLS', 'B0012GMY6Y', 'B000EMG9YU', 'B001662F8C', 'B000003F39', 'B000001CZE', 'B0001IXTIG', 'B000FOQ0KA', 'B00000DGUY', 'B0000000JS'], 'buy_after_viewing': ['B000002LRR', 'B002RV01QI', 'B000050XEI', 'B000002LRT']
    },
    'salesRank': {'Music': 939190},
    'categories': [
        ['CDs & Vinyl', 'New Age', 'Celtic New Age'], ['CDs & Vinyl', 'New Age', 'Meditation'], ['CDs & Vinyl', 'Pop'], ['CDs & Vinyl', 'Rock'], ['Digital Music', 'New Age', 'Celtic New Age']
    ]
}
```

## Algorithm

1. Find the number of unique reviewer IDs for each product from the review file. (Use pair RDD to accomplish this step, the key is the product ID/asin)
2. Create an RDD, based on the metadata, consisting of key/value-array pairs, key is the product ID/asin and value should contain the price of the product.
3. Join the pair RDD in Step 2 with the set of product-ID and unique reviewer ID count pairs calculated in Step 1.
4. Display the product ID, unique reviewer ID count, and the product price for the top 10 products based on the unique reviewer ID count.

## Notes
**Resilient Distributed Dataset (RDD)** A collection of elements partitioned across the nodes of the cluster that can be operated on in parallel. RDDs are created starting with a file in the HDFS, or an existing Scala collection in the driver program, and transforming it. Users may also ask Spark to persist an RDD in memory, allowing it to be reused efficiently across parallel operations. Finally, RDDs automatically recover from node failures.

**Using RDD** https://spark.apache.org/docs/latest/rdd-programming-guide.html#working-with-key-value-pairs