from scripts.rake import *
import json


# get review texts aggregated by asin id
def get_rdd(base, input, num_part):
                base_dir = os.path.join(base)
                input_path = os.path.join(input)
                file_name = os.path.join(base_dir, input_path)
                # load data
                rdd = sc.textFile(file_name, num_part)
                rdd_j = rdd.map(json.loads)
                rdd_j.cache()
                return rdd_j


num_part = 16
revs = get_rdd('data', 'reviews_electronics.json', num_part)
rev_texts = revs.map(lambda x: (x['asin'], x['reviewText']))
rev_agg_texts = rev_texts.map(lambda (asin, text): (asin, [text])).reduceByKey(lambda x, y: x + y)
rev_agg_texts.cache()

prods = get_rdd('data', 'meta_electronics.json', num_part)
categ = prods.map( lambda x: (x.get('asin'), x.get('categories')) )
categ = categ.flapMapValues(lambda x: x)
computers = categ_.filter( lambda (asin, cats): 'Computers & Accessories' in cats )
prods_ = prods_.join(computers)
prods.cache()

# (asin, ([review], (d_prod, [category])) )
items = rev_agg_texts.join(prods_)
items = items.map( lambda (asin, (reviews, (d_prod, categories))): (asin, reviews, d_prod, categories) )


# 1. RAKE: keyword. use rake algorithm to extract keywords and take top 10 keywords from each asin
rake = Rake('data/MergedStopList.txt') # TODO: add more into this list
items_wk = items.map( lambda (asin, reviews, d_prod, categories): (asin, rake.run(' '.join(reviews)), reviews, d_prod, categories) )

# 2. NP: noun phrasee among these keywords
import nltk
from scripts.np_extractor import *
items_wk.cache()
items_np = items_wk.map(lambda (asin, pairs, reviews, d_prod, categories): 
                               (asin, [(NPExtractor(string).extract(), score) for (string, score) in pairs], reviews, d_prod, categories)
                       )
items_np = items_np.map(lambda (asin, pairs, reviews, d_prod, categories):
                               (asin, [(toks, scr) for (toks, scr) in pairs if len(toks) > 0], reviews, d_prod, categories)
                       )



# 3. output
import pandas as pd
df = pd.DataFrame(items_np.collect())
df.to_csv('data/computers_kw.csv')
