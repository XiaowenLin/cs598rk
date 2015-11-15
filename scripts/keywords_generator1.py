# pyspark code for generating key word dict
import json
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF

def get_rdd(base, input, num_part):
    base_dir = os.path.join(base)
    input_path = os.path.join(input)
    file_name = os.path.join(base_dir, input_path)
    # load data
    rdd = sc.textFile(file_name, num_part)
    rdd_j = rdd.map(json.loads)
    rdd_j.cache()
    return rdd_j


revs = get_rdd('data', 'reviews_musical.json', 4)
prods = get_rdd('data', 'meta_musical.json', 4)

# Load documents (one per line).
rev_texts = ['cheese is good for your health', 'he like cheese', 'cheese is an important income resource here']
rev_texts = sc.parallelize(rev_texts).map(lambda line: line.split(" "))
#rev_texts = revs.map(lambda x: x['reviewText'].split(' '))

# term frequency
hashingTF = HashingTF()
tf = hashingTF.transform(rev_texts)

# ... continue from the previous example
tf.cache()
idf = IDF().fit(tf)
tfidf = idf.transform(tf)
# from sparsevector, get indices and values
# get indices -> term PairRDD
keys = set(rev_texts.reduce(lambda x, y: x + y))
terms_dict = sc.parallelize(keys)
terms_pair = terms_dict.map(lambda x: (hashingTF.indexOf(x), x))
