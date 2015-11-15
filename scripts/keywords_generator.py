# pyspark code for generating key word dict
from np_extractor import *
import json

# filename
#base_dir = os.path.join('data')
#input_path = os.path.join('reviews_musical.json')
#file_name = os.path.join(base_dir, input_path)

# load data
#num_part = 4
#r_prods = sc.textFile(file_name, num_part)
#prods = r_prods.map(json.loads)
#kwords = prods.map(lambda x: NPExtractor(x['reviewText']).extract())


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
# for the same asin, generate the key words
asin_rev = revs.map( lambda x: (x['asin'], NPExtractor(x['reviewText']).extract() )).reduceByKey(lambda x, y: x + y)
asin_rev.cache()
# for the same subcategory generate the key words
