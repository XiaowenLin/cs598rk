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


revs = get_rdd('data', 'reviews_electronics.json', 4)
rev_texts = revs.map(lambda x: (x['asin'], x['reviewText']))
rev_agg_texts = rev_texts.map(lambda (asin, text): (asin, [text])).reduceByKey(lambda x, y: x + y)


# 1. RAKE: keyword. use rake algorithm to extract keywords and take top 10 keywords from each asin
rake = Rake('data/SmartStoplist.txt')
rev_agg_rake = rev_agg_texts.map( lambda (asin, texts): (asin, rake.run(' '.join(texts))) )
rev_agg_rake10 = rev_agg_rake.map( lambda (asin, rakes): (asin, rakes[:10]) )
rev_agg_rake10.take(2)


# 2. NP: noun phrasee among these keywords
import nltk
from scripts.np_extractor import *
rev_agg_rake10.cache()
rev_np = rev_agg_rake10.map(lambda (asin, pairs): [(NPExtractor(string).extract(), score) for (string, score) in pairs])
rev_np = rev_np.map(lambda lst: [(np_lst, score) for (np_lst, score) in lst if len(np_lst) > 0])
rev_np = rev_np.filter(lambda lst: len(lst) > 0)

# 3. output
import pandas as pd
df = pf.DataFrame(rev_np.collect())
df.to_csv('data/electronics_kw.csv')
